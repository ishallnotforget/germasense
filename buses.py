#!/usr/bin/env python3
"""Estimated live bus and train positions for all of Germany.

Nobody publishes GPS positions. Instead we combine:
  * the national timetable (gtfs.de, checked daily for a new version) -> every trip that should be on the road right now, and
  * the free Germany-wide GTFS-Realtime feed (gtfs.de, ~minutely) -> live times/delays for the trips that report them.
A vehicle between two stops is placed proportionally along the straight line between them.
Trips without realtime data are still shown, flagged as timetable-only.

Built for small servers: each trip's stop times are packed into one binary row, an R-tree over
(time, lat, lon) finds the trips that cross the visible map area, and the realtime feed is only
indexed by trip id; a trip's updates are decoded when that trip is actually on screen.

Usage:
    python3 buses.py            # print the vehicles currently around Osnabrück
    python3 buses.py refresh    # force a refresh of the static timetable cache (~270 MB download)

Only stdlib: the protobuf subset we need is decoded by hand.
"""
import csv
import datetime as dt
import email.utils
import gzip
import io
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
from array import array
from pathlib import Path
from zoneinfo import ZoneInfo

STATIC_URL = "https://download.gtfs.de/germany/free/latest.zip"
RT_URL = "https://realtime.gtfs.de/realtime-free.pb"
GTFS_DB = Path(__file__).with_name("gtfs-de.db")
RT_MIN_INTERVAL_S = 60
STATIC_MAX_AGE_S = 7 * 24 * 3600  # rebuild at least this often, even if the server doesn't say it changed
STATIC_CHECK_S = 24 * 3600  # look this often for a newer timetable (one HEAD request); rebuild only if there is one
MAX_DELAY_S = 1800  # look this far around the timetable for trips that may still be running late
VEHICLE_CACHE_S = 10
MAX_ALL_MODES_DEG2 = 12  # larger requested areas get trains only, so a zoomed-out Germany stays light
TZ = ZoneInfo("Europe/Berlin")
UA = "germasense/0.2 (+https://github.com/ishallnotforget)"


def is_rail(route_type):
    return route_type == 2 or 100 <= route_type < 200


# --- static timetable cache -------------------------------------------------------------------

class RemoteFile(io.RawIOBase):
    """Seekable file over HTTP Range requests, so zipfile can pull single members of the remote zip."""

    def __init__(self, url):
        self.url, self.pos = url, 0
        head = urllib.request.urlopen(urllib.request.Request(url, method="HEAD", headers={"User-Agent": UA}), timeout=60)
        self.size = int(head.headers["Content-Length"])
        self.last_modified = head.headers.get("Last-Modified")

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        return self.pos

    def readinto(self, b):
        if self.pos >= self.size or not len(b):
            return 0
        end = min(self.size, self.pos + len(b)) - 1
        req = urllib.request.Request(self.url, headers={"Range": f"bytes={self.pos}-{end}", "User-Agent": UA})
        for attempt in range(4):
            try:
                data = urllib.request.urlopen(req, timeout=120).read()
                break
            except (urllib.error.URLError, TimeoutError):
                if attempt == 3:
                    raise
                time.sleep(5)
        b[:len(data)] = data
        self.pos += len(data)
        return len(data)


def static_meta(key):
    if not GTFS_DB.exists():
        return None
    db = sqlite3.connect(GTFS_DB)
    try:
        row = db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    except sqlite3.OperationalError:
        row = None
    db.close()
    return row[0] if row else None


def static_age_s():
    fetched = static_meta("fetched")
    return time.time() - float(fetched) if fetched else float("inf")


def static_changed():
    """True if gtfs.de has published a timetable other than the one in our cache (or doesn't say)."""
    head = urllib.request.urlopen(urllib.request.Request(STATIC_URL, method="HEAD", headers={"User-Agent": UA}),
                                  timeout=60)
    remote = head.headers.get("Last-Modified")
    return not remote or remote != static_meta("source_modified")


def hms(s):
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def refresh_static(log=lambda *a: None):
    """Rebuild gtfs.db from the national GTFS zip.

    trip_times holds one row per trip: its time span, bounding box and a packed int32 array
    [stop indexes..., arrivals..., departures...]. trip_rt / trip_rt_rail are R-trees over it.
    """
    remote = RemoteFile(STATIC_URL)
    z = zipfile.ZipFile(io.BufferedReader(remote, buffer_size=4 << 20))
    rows = lambda name: csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8-sig"))
    tmp = GTFS_DB.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    db = sqlite3.connect(tmp)
    db.executescript("""
        PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;
        CREATE TABLE stops (idx INTEGER PRIMARY KEY, stop_id TEXT, name TEXT, lat REAL, lon REAL);
        CREATE TABLE routes (route_id TEXT PRIMARY KEY, name TEXT, agency TEXT, type INTEGER);
        CREATE TABLE trips (trip_id TEXT PRIMARY KEY, route_id TEXT, service_id TEXT) WITHOUT ROWID;
        CREATE TABLE calendar (service_id TEXT, days TEXT, start TEXT, end TEXT);
        CREATE TABLE calendar_dates (service_id TEXT, date TEXT, exception INTEGER);
        CREATE TABLE trip_times (id INTEGER PRIMARY KEY, trip_id TEXT, data BLOB);
        CREATE VIRTUAL TABLE trip_rt USING rtree (id, t0, t1, lat0, lat1, lon0, lon1);
        CREATE VIRTUAL TABLE trip_rt_rail USING rtree (id, t0, t1, lat0, lat1, lon0, lon1);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    """)
    stop_idx, lats, lons, stop_rows = {}, array("d"), array("d"), []
    for r in rows("stops.txt"):
        try:
            lat, lon = float(r["stop_lat"]), float(r["stop_lon"])
        except ValueError:
            continue
        stop_idx[r["stop_id"]] = len(lats)
        stop_rows.append((len(lats), r["stop_id"], r["stop_name"], lat, lon))
        lats.append(lat)
        lons.append(lon)
    db.executemany("INSERT INTO stops VALUES (?,?,?,?,?)", stop_rows)
    del stop_rows
    log(f"{len(lats)} stops")
    agencies = {r["agency_id"]: r["agency_name"] for r in rows("agency.txt")}
    db.executemany("INSERT OR IGNORE INTO routes VALUES (?,?,?,?)",
                   ((r["route_id"], r["route_short_name"] or r["route_long_name"], agencies.get(r["agency_id"], ""),
                     int(r["route_type"] or 3)) for r in rows("routes.txt")))
    db.executemany("INSERT INTO calendar VALUES (?,?,?,?)",
                   ((r["service_id"], "".join(r[d] for d in ("monday", "tuesday", "wednesday", "thursday", "friday",
                                                             "saturday", "sunday")), r["start_date"], r["end_date"])
                    for r in rows("calendar.txt")))
    db.executemany("INSERT INTO calendar_dates VALUES (?,?,?)",
                   ((r["service_id"], r["date"], int(r["exception_type"])) for r in rows("calendar_dates.txt")))
    db.executemany("INSERT OR IGNORE INTO trips VALUES (?,?,?)",
                   ((r["trip_id"], r["route_id"], r["service_id"]) for r in rows("trips.txt")))
    db.commit()
    log("routes, calendars and trips loaded; packing stop_times")

    # stop_times is ~2 GB raw and arrives grouped by trip: pack each trip as soon as the next one starts.
    reader = csv.reader(io.TextIOWrapper(z.open("stop_times.txt"), encoding="utf-8-sig"), )
    head = next(reader)
    ti, ai, di, si, qi = (head.index(c) for c in ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"))
    batch, rtree, n_trips = [], [], 0
    cur_trip, cur = None, []

    def flush():
        nonlocal n_trips
        cur.sort()
        idx = [c[1] for c in cur]
        arr = [c[2] for c in cur]
        dep = [c[3] for c in cur]
        packed = array("i", idx + arr + dep).tobytes()
        la = [lats[i] for i in idx]
        lo = [lons[i] for i in idx]
        n_trips += 1
        batch.append((n_trips, cur_trip, packed))
        rtree.append((n_trips, min(dep), max(arr), min(la), max(la), min(lo), max(lo)))
        if len(batch) >= 20000:
            db.executemany("INSERT INTO trip_times VALUES (?,?,?)", batch)
            db.executemany("INSERT INTO trip_rt VALUES (?,?,?,?,?,?,?)", rtree)
            batch.clear()
            rtree.clear()

    for r in reader:
        if r[ti] != cur_trip:
            if len(cur) >= 2:
                flush()
            cur_trip, cur = r[ti], []
        s = stop_idx.get(r[si])
        if s is None:
            continue
        a = hms(r[ai] or r[di])
        cur.append((int(r[qi]), s, a, hms(r[di]) if r[di] else a))
    if len(cur) >= 2:
        flush()
    db.executemany("INSERT INTO trip_times VALUES (?,?,?)", batch)
    db.executemany("INSERT INTO trip_rt VALUES (?,?,?,?,?,?,?)", rtree)
    log(f"{n_trips} trips packed; building indexes")
    rail = ",".join(str(t) for t in [2] + list(range(100, 200)))
    db.executescript(f"""
        INSERT INTO trip_rt_rail SELECT x.* FROM trip_rt x JOIN trip_times tt ON tt.id = x.id
            JOIN trips t ON t.trip_id = tt.trip_id JOIN routes r ON r.route_id = t.route_id WHERE r.type IN ({rail});
        CREATE INDEX calendar_dates_date ON calendar_dates (date);
    """)
    db.executemany("INSERT INTO meta VALUES (?,?)", [("fetched", str(time.time())), ("source_modified", remote.last_modified)])
    db.commit()
    db.execute("VACUUM")
    db.close()
    tmp.replace(GTFS_DB)


# --- minimal GTFS-Realtime protobuf decoding ----------------------------------------------------

def varint(buf, i):
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if b < 0x80:
            return result, i
        shift += 7


def fields(buf):
    """Yield (field_number, value) for one message; length-delimited values are memoryview slices."""
    i, n = 0, len(buf)
    while i < n:
        key, i = varint(buf, i)
        num, wt = key >> 3, key & 7
        if wt == 0:
            v, i = varint(buf, i)
        elif wt == 2:
            ln, i = varint(buf, i)
            v, i = buf[i:i + ln], i + ln
        elif wt == 1:
            v, i = buf[i:i + 8], i + 8
        elif wt == 5:
            v, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"unsupported wire type {wt}")
        yield num, v


def signed(v):
    return v - (1 << 64) if v >= 1 << 63 else v


def stop_event(buf):
    delay = t = None
    for num, v in fields(buf):
        if num == 1:
            delay = signed(v)
        elif num == 2:
            t = signed(v)
    return delay, t


def parse_stop_updates(buf):
    """Decode one TripUpdate's StopTimeUpdates: [(stop_id, (arr_delay, arr_time), (dep_delay, dep_time), skipped)]."""
    stus = []
    for num, v in fields(buf):
        if num == 2:
            stop_id, arr, dep, skipped = None, (None, None), (None, None), False
            for n2, v2 in fields(v):
                if n2 == 4:
                    stop_id = bytes(v2).decode()
                elif n2 == 2:
                    arr = stop_event(v2)
                elif n2 == 3:
                    dep = stop_event(v2)
                elif n2 == 5 and v2 == 1:  # SKIPPED
                    skipped = True
            stus.append((stop_id, arr, dep, skipped))
    return stus


def index_trip_updates(feed):
    """Only read each TripUpdate's TripDescriptor: returns ({trip_id: raw TripUpdate}, cancelled trip ids)."""
    buf = memoryview(feed)
    raw, cancelled = {}, set()
    for num, entity in fields(buf):
        if num != 2:
            continue
        for n2, tu in fields(entity):
            if n2 != 3:
                continue
            for n3, desc in fields(tu):
                if n3 == 1:
                    trip_id, canc = None, False
                    for n4, v4 in fields(desc):
                        if n4 == 1:
                            trip_id = bytes(v4).decode()
                        elif n4 == 4 and v4 == 3:  # CANCELED
                            canc = True
                    if trip_id:
                        (cancelled.add(trip_id) if canc else raw.__setitem__(trip_id, tu))
                    break  # the descriptor comes first; the stop updates are decoded later, on demand
    return raw, cancelled


# --- live state --------------------------------------------------------------------------------

def service_midnight(date):
    return dt.datetime.combine(date, dt.time(), TZ).timestamp()


class Buses:
    """Fetches the realtime feed at most once a minute (only when asked) and merges it with the timetable."""

    def __init__(self):
        self.lock = threading.Lock()
        self.fetched_at = 0.0
        self.last_modified = None
        self.feed_time = None
        self.rt_raw = {}        # trip_id -> undecoded TripUpdate
        self.rt = {}            # trip_id -> decoded stop updates, filled on demand
        self.cancelled = set()
        self.error = None
        self.static_error = None
        self.services = {}      # date -> set of active service_ids
        self.static_thread = None
        self.static_checked = 0.0
        self.cache = {}         # (bbox, rail_only) -> (time, result)
        self.stops, self.stops_db_mtime = None, None

    # static timetable: refreshed in the background, the map keeps working meanwhile
    def _ensure_static(self):
        """Rebuild the cache when it is missing, a week old, or gtfs.de has published a new timetable.
        The upstream data sometimes has gaps (e.g. a city's weekday lines missing), so a fixed version
        should reach the map within a day rather than a week."""
        if self.static_thread and self.static_thread.is_alive():
            return
        age = static_age_s()
        if age < STATIC_MAX_AGE_S and time.time() - self.static_checked < STATIC_CHECK_S:
            return
        self.static_checked = time.time()

        def run():
            try:
                if age < STATIC_MAX_AGE_S and not static_changed():
                    return
                refresh_static()
                self.services.clear()
                self.static_error = None
            except Exception as e:
                self.static_error = f"{type(e).__name__}: {e}"

        self.static_thread = threading.Thread(target=run, daemon=True)
        self.static_thread.start()

    def _fetch(self):
        headers = {"User-Agent": UA, "Accept-Encoding": "gzip"}
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        try:
            r = urllib.request.urlopen(urllib.request.Request(RT_URL, headers=headers), timeout=60)
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return None
            raise
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
        self.last_modified = r.headers.get("Last-Modified")
        return data

    def refresh(self):
        self._ensure_static()
        with self.lock:
            if time.time() - self.fetched_at < RT_MIN_INTERVAL_S:
                return
            self.fetched_at = time.time()
            try:
                feed = self._fetch()
                if feed is not None:
                    self.rt_raw, self.cancelled = index_trip_updates(feed)
                    self.rt = {}
                    self.cache.clear()
                    self.feed_time = email.utils.parsedate_to_datetime(self.last_modified).isoformat()
                self.error = None
            except Exception as e:
                self.error = f"{type(e).__name__}: {e}"

    def _updates(self, trip_id):
        if trip_id not in self.rt:
            raw = self.rt_raw.get(trip_id)
            self.rt[trip_id] = parse_stop_updates(raw) if raw is not None else None
        return self.rt[trip_id]

    def _active_services(self, db, date):
        if date not in self.services:
            d, wd = date.strftime("%Y%m%d"), date.weekday()
            active = {sid for sid, days, start, end in db.execute("SELECT * FROM calendar")
                      if start <= d <= end and days[wd] == "1"}
            for sid, exc in db.execute("SELECT service_id, exception FROM calendar_dates WHERE date = ?", (d,)):
                (active.add if exc == 1 else active.discard)(sid)
            self.services[date] = active
        return self.services[date]

    @staticmethod
    def _timeline(sched, rt):
        """Merge scheduled stops [(stop_id, arr, dep)] with realtime updates. Delays propagate downstream."""
        updates = {}
        for stop_id, (ad, at), (dd, dtm), skipped in rt or []:
            updates[stop_id] = (ad, at, dd, dtm, skipped)
        out, delay = [], None
        for stop_id, arr, dep in sched:
            u = updates.get(stop_id)
            if u:
                ad, at, dd, dtm, skipped = u
                if skipped:
                    continue
                a = at or (arr + ad if ad is not None else None)
                d = dtm or (dep + dd if dd is not None else None)
                a, d = a or d, d or a
                if a is not None:
                    delay = d - dep
                    out.append((stop_id, a, d, delay))
                    continue
            out.append((stop_id, arr + (delay or 0), dep + (delay or 0), delay))
        return out

    def vehicles(self, now=None, bbox=None, rail_only=False):
        """Current segment of every vehicle in bbox (lat0, lon0, lat1, lon1).

        Areas larger than MAX_ALL_MODES_DEG2 get trains only. Results are cached per area for
        VEHICLE_CACHE_S; the client animates in between.
        """
        self.refresh()
        now = now or time.time()
        if static_age_s() == float("inf"):
            return {"now": now, "feed_time": self.feed_time, "error": self.error or self.static_error,
                    "loading": True, "vehicles": []}
        bbox = bbox or (47.2, 5.8, 55.1, 15.1)
        lat0, lon0, lat1, lon1 = bbox
        rail_only = rail_only or (lat1 - lat0) * (lon1 - lon0) > MAX_ALL_MODES_DEG2
        key = (tuple(round(x, 2) for x in bbox), rail_only)
        with self.lock:
            hit = self.cache.get(key)
            if not hit or now - hit[0] > VEHICLE_CACHE_S:
                if len(self.cache) > 64:
                    self.cache.clear()
                hit = (now, self._compute(now, bbox, rail_only))
                self.cache[key] = hit
        return {**hit[1], "now": now, "rail_only": rail_only}

    def _load_stops(self, db):
        if self.stops is None or self.stops_db_mtime != GTFS_DB.stat().st_mtime:
            ids, names, lats, lons = [], [], array("d"), array("d")
            for _, sid, name, lat, lon in db.execute("SELECT * FROM stops ORDER BY idx"):
                ids.append(sid)
                names.append(name)
                lats.append(lat)
                lons.append(lon)
            self.stops = (ids, names, lats, lons)
            self.stops_db_mtime = GTFS_DB.stat().st_mtime
            self.services.clear()
        return self.stops

    def _compute(self, now, bbox, rail_only):
        db = sqlite3.connect(f"file:{GTFS_DB}?mode=ro", uri=True)
        ids, names, lats, lons = self._load_stops(db)
        lat0, lon0, lat1, lon1 = bbox
        inside = lambda i: lat0 <= lats[i] <= lat1 and lon0 <= lons[i] <= lon1
        today = dt.datetime.fromtimestamp(now, TZ).date()
        out, seen = [], set()
        for date in (today, today - dt.timedelta(days=1)):
            midnight = service_midnight(date)
            services = self._active_services(db, date)
            t = now - midnight
            for trip_id, data, service_id, line, agency, rtype in db.execute(f"""
                    SELECT tt.trip_id, tt.data, t.service_id, r.name, r.agency, r.type
                    FROM {"trip_rt_rail" if rail_only else "trip_rt"} x
                    JOIN trip_times tt ON tt.id = x.id
                    JOIN trips t ON t.trip_id = tt.trip_id LEFT JOIN routes r ON r.route_id = t.route_id
                    WHERE x.t0 <= ? AND x.t1 >= ? AND x.lat0 <= ? AND x.lat1 >= ? AND x.lon0 <= ? AND x.lon1 >= ?""",
                    (t + 300, t - MAX_DELAY_S, lat1, lat0, lon1, lon0)):
                if service_id not in services or trip_id in self.cancelled or trip_id in seen:
                    continue
                a = array("i")
                a.frombytes(data)
                n = len(a) // 3
                idx = a[:n]
                sched = [(ids[s], midnight + ar, midnight + dp) for s, ar, dp in zip(idx, a[n:2 * n], a[2 * n:])]
                rt = self._updates(trip_id)
                tl = self._timeline(sched, rt)
                for k in range(len(tl) - 1):
                    p, q = tl[k], tl[k + 1]
                    if p[2] <= now <= q[1] or p[1] <= now <= p[2]:
                        break
                else:
                    continue
                pos = {s: i for s, i in zip((ids[i] for i in idx), idx)}
                ia, ib = pos[p[0]], pos[q[0]]
                if not (inside(ia) or inside(ib)):
                    continue
                seen.add(trip_id)
                live = rt is not None
                out.append({"trip": trip_id, "line": line or "?", "agency": agency or "", "type": rtype or 3,
                            "headsign": names[pos[tl[-1][0]]], "live": live, "delay": q[3] if live else None,
                            "from": [lats[ia], lons[ia]], "to": [lats[ib], lons[ib]],
                            "dep": p[2], "arr": q[1], "prev_stop": names[ia], "next_stop": names[ib]})
        db.close()
        return {"feed_time": self.feed_time, "error": self.error or self.static_error, "vehicles": out}


if __name__ == "__main__":
    if sys.argv[1:] == ["refresh"] or static_age_s() == float("inf"):
        print("Fetching timetable (one-off ~270 MB, several minutes)…")
        t0 = time.time()
        refresh_static(log=lambda m: print(f"  {time.time() - t0:5.0f}s {m}"))
    b = Buses()
    t0 = time.time()
    res = b.vehicles(bbox=(52.15, 7.9, 52.4, 8.2))  # Osnabrück
    print(f"computed in {time.time() - t0:.2f}s")
    if res["error"]:
        print("ERROR", res["error"])
    live = sum(v["live"] for v in res["vehicles"])
    print(f"{len(res['vehicles'])} vehicles around Osnabrück ({live} with live data), feed {res['feed_time']}")
    for v in sorted(res["vehicles"], key=lambda v: v["line"])[:40]:
        d = f"{round(v['delay'] / 60):+d} min" if v["live"] and v["delay"] is not None else "timetable"
        print(f"  {v['line']:>6} → {v['headsign'][:26]:26} {v['prev_stop'][:22]:22} → {v['next_stop'][:22]:22} {d}")
