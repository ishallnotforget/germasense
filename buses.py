#!/usr/bin/env python3
"""Estimated live bus and train positions around Osnabrück.

Nobody publishes GPS positions here. Instead we combine:
  * the national timetable (gtfs.de, weekly) -> every trip that should be on the road right now, and
  * the free Germany-wide GTFS-Realtime feed (gtfs.de, ~minutely) -> live times/delays for the trips that report them.
A vehicle between two stops is placed proportionally along the straight line between them.
Trips without realtime data are still shown, flagged as timetable-only.

Usage:
    python3 buses.py            # print the vehicles currently in the area
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
from pathlib import Path
from zoneinfo import ZoneInfo

STATIC_URL = "https://download.gtfs.de/germany/free/latest.zip"
RT_URL = "https://realtime.gtfs.de/realtime-free.pb"
GTFS_DB = Path(__file__).with_name("gtfs.db")
SHOW_BBOX = (51.38, 6.85, 52.80, 8.54)  # vehicles shown: same area as collector.BBOX (Quakenbrück to Essen)
STOP_BBOX = (51.28, 6.65, 52.90, 8.70)  # stops kept: area + margin; plus each trip's first stop beyond it
RT_MIN_INTERVAL_S = 60
STATIC_MAX_AGE_S = 7 * 24 * 3600
MAX_DELAY_S = 1800  # look this far around the timetable for trips that may still be running late
VEHICLE_CACHE_S = 10
TZ = ZoneInfo("Europe/Berlin")
UA = "osna-sensors/0.2 (private research collector)"


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


def static_age_s():
    if not GTFS_DB.exists():
        return float("inf")
    db = sqlite3.connect(GTFS_DB)
    try:
        row = db.execute("SELECT value FROM meta WHERE key = 'fetched'").fetchone()
    except sqlite3.OperationalError:
        row = None
    db.close()
    return time.time() - float(row[0]) if row else float("inf")


def hms(s):
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def refresh_static():
    """Rebuild gtfs.db from the national GTFS zip, keeping only stops and stop times inside STOP_BBOX."""
    remote = RemoteFile(STATIC_URL)
    z = zipfile.ZipFile(io.BufferedReader(remote, buffer_size=4 << 20))
    rows = lambda name: csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8-sig"))
    tmp = GTFS_DB.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    db = sqlite3.connect(tmp)
    db.executescript("""
        CREATE TABLE stops (stop_id TEXT PRIMARY KEY, name TEXT, lat REAL, lon REAL);
        CREATE TABLE routes (route_id TEXT PRIMARY KEY, name TEXT, agency TEXT, type INTEGER);
        CREATE TABLE trips (trip_id TEXT PRIMARY KEY, route_id TEXT, service_id TEXT) WITHOUT ROWID;
        CREATE TABLE calendar (service_id TEXT, days TEXT, start TEXT, end TEXT);
        CREATE TABLE calendar_dates (service_id TEXT, date TEXT, exception INTEGER);
        CREATE TABLE stop_times (trip_id TEXT, seq INTEGER, stop_id TEXT, arr INTEGER, dep INTEGER,
                                 PRIMARY KEY (trip_id, seq)) WITHOUT ROWID;
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    """)
    lat0, lon0, lat1, lon1 = STOP_BBOX
    all_stops, stops = {}, set()
    for r in rows("stops.txt"):
        try:
            lat, lon = float(r["stop_lat"]), float(r["stop_lon"])
        except ValueError:
            continue
        all_stops[r["stop_id"]] = (r["stop_name"], lat, lon)
        if lat0 < lat < lat1 and lon0 < lon < lon1:
            stops.add(r["stop_id"])
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
    # stop_times is ~2 GB raw: plain csv.reader with fixed column positions, keep only stops in the area.
    # Rows come grouped by trip in stop order, so we also keep the stop just before entering and just after
    # leaving the area: a train from Osnabrück to Bremen then stays on the map until it is out of view.
    reader = csv.reader(io.TextIOWrapper(z.open("stop_times.txt"), encoding="utf-8-sig"))
    head = next(reader)
    ti, ai, di, si, qi = (head.index(c) for c in ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"))
    trip_ids, used_stops, batch = set(), set(), []

    def keep(r):
        arr = hms(r[ai] or r[di])
        batch.append((r[ti], int(r[qi]), r[si], arr, hms(r[di]) if r[di] else arr))
        trip_ids.add(r[ti])
        used_stops.add(r[si])

    prev, prev_in, prev_kept = None, False, False
    for r in reader:
        inside = r[si] in stops
        same_trip = prev is not None and prev[ti] == r[ti]
        if same_trip and inside and not prev_in and not prev_kept:
            keep(prev)  # entering the area
        if inside or (same_trip and prev_in):  # inside, or the first stop after leaving
            keep(r)
            kept = True
        else:
            kept = False
        prev, prev_in, prev_kept = r, inside, kept
        if len(batch) >= 50000:
            db.executemany("INSERT OR IGNORE INTO stop_times VALUES (?,?,?,?,?)", batch)
            batch.clear()
    db.executemany("INSERT OR IGNORE INTO stop_times VALUES (?,?,?,?,?)", batch)
    db.executemany("INSERT OR IGNORE INTO stops VALUES (?,?,?,?)",
                   ((sid, *all_stops[sid]) for sid in used_stops | stops if sid in all_stops))
    db.executemany("INSERT OR IGNORE INTO trips VALUES (?,?,?)",
                   ((r["trip_id"], r["route_id"], r["service_id"]) for r in rows("trips.txt") if r["trip_id"] in trip_ids))
    db.executescript("""
        CREATE TABLE trip_span AS
            SELECT trip_id, MIN(dep) AS start_s, MAX(arr) AS end_s FROM stop_times GROUP BY trip_id;
        CREATE INDEX trip_span_time ON trip_span (start_s, end_s);
        CREATE INDEX calendar_dates_date ON calendar_dates (date);
    """)
    db.executemany("INSERT INTO meta VALUES (?,?)", [("fetched", str(time.time())), ("source_modified", remote.last_modified)])
    db.commit()
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


def parse_trip_update(buf):
    """Return (trip_id, cancelled, [(stop_id, (arr_delay, arr_time), (dep_delay, dep_time), skipped)])."""
    trip_id, cancelled, stus = None, False, []
    for num, v in fields(buf):
        if num == 1:  # TripDescriptor
            for n2, v2 in fields(v):
                if n2 == 1:
                    trip_id = bytes(v2).decode()
                elif n2 == 4 and v2 == 3:  # CANCELED
                    cancelled = True
        elif num == 2:  # StopTimeUpdate
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
    return trip_id, cancelled, stus


def iter_trip_updates(feed):
    buf = memoryview(feed)
    for num, entity in fields(buf):
        if num != 2:
            continue
        for n2, v2 in fields(entity):
            if n2 == 3:
                yield parse_trip_update(v2)


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
        self.rt = {}            # trip_id -> list of stop time updates
        self.cancelled = set()
        self.error = None
        self.static_error = None
        self.services = {}      # date -> set of active service_ids
        self.static_thread = None
        self.cache, self.cache_at = None, 0.0
        self.stops, self.stops_db_mtime = None, None

    # static timetable: refreshed in the background, the map keeps working meanwhile
    def _ensure_static(self):
        if static_age_s() < STATIC_MAX_AGE_S or (self.static_thread and self.static_thread.is_alive()):
            return

        def run():
            try:
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
                    rt, cancelled = {}, set()
                    for trip_id, canc, stus in iter_trip_updates(feed):
                        (cancelled.add(trip_id) if canc else rt.__setitem__(trip_id, stus))
                    self.rt, self.cancelled = rt, cancelled
                    self.feed_time = email.utils.parsedate_to_datetime(self.last_modified).isoformat()
                self.error = None
            except Exception as e:
                self.error = f"{type(e).__name__}: {e}"

    def _active_services(self, db, date):
        if date not in self.services:
            d, wd = date.strftime("%Y%m%d"), date.weekday()
            active = {sid for sid, days, start, end in db.execute("SELECT * FROM calendar")
                      if start <= d <= end and days[wd] == "1"}
            for sid, exc in db.execute("SELECT service_id, exception FROM calendar_dates WHERE date = ?", (d,)):
                (active.add if exc == 1 else active.discard)(sid)
            self.services[date] = active
        return self.services[date]

    def _timeline(self, sched, rt):
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

    def vehicles(self, now=None, bbox=None):
        """Current segment per vehicle, optionally only those touching bbox (lat0, lon0, lat1, lon1).

        The full list is computed at most every VEHICLE_CACHE_S; the client animates in between.
        """
        self.refresh()
        now = now or time.time()
        if static_age_s() == float("inf"):
            return {"now": now, "feed_time": self.feed_time, "error": self.error or self.static_error,
                    "loading": True, "vehicles": []}
        with self.lock:
            if now - self.cache_at > VEHICLE_CACHE_S:
                self.cache, self.cache_at = self._compute(now), now
            res = self.cache
        if bbox:
            lat0, lon0, lat1, lon1 = bbox
            inside = lambda p: lat0 <= p[0] <= lat1 and lon0 <= p[1] <= lon1
            res = {**res, "vehicles": [v for v in res["vehicles"] if inside(v["from"]) or inside(v["to"])]}
        return {**res, "now": now}

    def _compute(self, now):
        db = sqlite3.connect(GTFS_DB)
        if self.stops is None or self.stops_db_mtime != GTFS_DB.stat().st_mtime:
            self.stops = {sid: (name, lat, lon) for sid, name, lat, lon in db.execute("SELECT * FROM stops")}
            self.stops_db_mtime = GTFS_DB.stat().st_mtime
        stops = self.stops
        today = dt.datetime.fromtimestamp(now, TZ).date()
        lat0, lon0, lat1, lon1 = SHOW_BBOX
        out, seen = [], set()
        for date in (today, today - dt.timedelta(days=1)):
            midnight = service_midnight(date)
            services = self._active_services(db, date)
            t = now - midnight
            # one query for all candidate trips' stop times instead of one per trip (thousands in the Ruhr)
            info, sched_by_trip = {}, {}
            for trip_id, service_id, line, agency, rtype, stop_id, arr, dep in db.execute("""
                    SELECT s.trip_id, t.service_id, r.name, r.agency, r.type, st.stop_id, st.arr, st.dep
                    FROM trip_span s JOIN trips t USING (trip_id) LEFT JOIN routes r USING (route_id)
                    JOIN stop_times st ON st.trip_id = s.trip_id
                    WHERE s.start_s <= ? AND s.end_s >= ? ORDER BY st.trip_id, st.seq""",
                    (t + 300, t - MAX_DELAY_S)):
                if service_id not in services:
                    continue
                info[trip_id] = (line, agency, rtype)
                sched_by_trip.setdefault(trip_id, []).append((stop_id, midnight + arr, midnight + dep))
            for trip_id, sched in sched_by_trip.items():
                if trip_id in self.cancelled or trip_id in seen:
                    continue
                line, agency, rtype = info[trip_id]
                live = trip_id in self.rt
                tl = self._timeline(sched, self.rt.get(trip_id))
                for a, b in zip(tl, tl[1:]):
                    if a[2] <= now <= b[1] or a[1] <= now <= a[2]:
                        break
                else:
                    continue
                pa, pb = stops[a[0]], stops[b[0]]
                if not any(lat0 < p[1] < lat1 and lon0 < p[2] < lon1 for p in (pa, pb)):
                    continue
                seen.add(trip_id)
                out.append({"trip": trip_id, "line": line or "?", "agency": agency or "", "type": rtype or 3,
                            "headsign": stops[tl[-1][0]][0], "live": live, "delay": b[3] if live else None,
                            "from": [pa[1], pa[2]], "to": [pb[1], pb[2]],
                            "dep": a[2], "arr": b[1], "prev_stop": pa[0], "next_stop": pb[0]})
        db.close()
        return {"feed_time": self.feed_time, "error": self.error or self.static_error, "vehicles": out}


if __name__ == "__main__":
    if sys.argv[1:] == ["refresh"] or static_age_s() == float("inf"):
        print("Fetching timetable (one-off ~270 MB, a few minutes)…")
        refresh_static()
    res = Buses().vehicles()
    if res["error"]:
        print("ERROR", res["error"])
    live = sum(v["live"] for v in res["vehicles"])
    print(f"{len(res['vehicles'])} vehicles ({live} with live data), feed {res['feed_time']}")
    for v in sorted(res["vehicles"], key=lambda v: v["line"]):
        if v["agency"].startswith("Verkehrsgemeinschaft Osna"):
            d = f"{round(v['delay'] / 60):+d} min" if v["live"] and v["delay"] is not None else "timetable"
            print(f"  {v['line']:>6} → {v['headsign'][:26]:26} {v['prev_stop'][:22]:22} → {v['next_stop'][:22]:22} {d}")
