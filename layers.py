#!/usr/bin/env python3
"""Live map overlays that are not sensor readings: aircraft, Autobahn traffic, civil-protection warnings, earthquakes.

Nothing is stored. Each layer is fetched only while someone has it open: the first request waits for the
fetch, later requests get the cached copy and trigger a background refresh once it is older than its TTL.
DWD rain radar and weather warnings need no server part; the browser loads them as WMS images from DWD.

    python3 layers.py            # fetch every layer once and print a summary
"""
import gzip
import html
import http.client
import json
import re
import threading
from collections import Counter
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

from collector import BBOX, UA, epoch, get, get_json, in_bbox

AUTOBAHN_SERVICES = {"roadworks": "roadworks", "closure": "closure", "warning": "warning",
                     "electric_charging_station": "charging"}
NINA = "https://warnung.bund.de/api31"
NINA_PROVIDERS = ("mowas", "katwarn", "biwapp", "lhp", "police")  # DWD weather warnings come from the WMS layer


def text(lines, limit=600):
    """Autobahn/NINA descriptions (list of lines or HTML) -> plain text, blank lines collapsed, shortened."""
    s = "\n".join(lines) if isinstance(lines, list) else re.sub(r"<br\s*/?>", "\n", lines or "")
    s = html.unescape(re.sub(r"<[^>]+>", "", s))
    s = re.sub(r"\n\s*\n+", "\n", s).strip()
    return s if len(s) <= limit else s[:limit].rsplit(" ", 1)[0] + " …"


def flights():
    """Aircraft within 250 NM of the centre of Germany (adsb.lol, ODbL). One request covers the whole country."""
    data = get_json("https://api.adsb.lol/v2/point/51.2/10.4/250", timeout=20)
    rows = []
    for a in data.get("ac", []):
        lat, lon = a.get("lat"), a.get("lon")
        if not in_bbox(lat, lon):
            continue
        alt = a.get("alt_baro")
        rows.append([a["hex"], round(lat, 4), round(lon, 4), 0 if alt == "ground" else alt, a.get("gs"),
                     a.get("track"), (a.get("flight") or "").strip(), a.get("t") or "", a.get("r") or "",
                     round(a.get("seen_pos") or 0, 1)])
    # [hex, lat, lon, altitude ft (0 = on ground), ground speed kt, track °, callsign, type, registration, age s]
    return {"now": data.get("now", time.time() * 1000) / 1000, "rows": rows}


_tls = threading.local()


def autobahn_get(path):
    """GET on verkehr.autobahn.de over one kept-alive connection per thread: the API stalls new TLS handshakes
    when ~450 of them arrive within a minute, but answers requests on an open connection in ~0.1 s."""
    for attempt in range(3):
        con = getattr(_tls, "con", None)
        if con is None:
            con = _tls.con = http.client.HTTPSConnection("verkehr.autobahn.de", timeout=15)
        try:
            con.request("GET", path, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
            r = con.getresponse()
            data = r.read()
            if r.status != 200:
                raise OSError(f"HTTP {r.status}")
            return json.loads(gzip.decompress(data) if r.getheader("Content-Encoding") == "gzip" else data)
        except Exception:
            con.close()
            _tls.con = None
            if attempt == 2:
                raise


def autobahn():
    """Current roadworks, closures, traffic warnings and charging stations on every Autobahn (~450 small requests)."""
    roads = autobahn_get("/o/autobahn/")["roads"]
    jobs = [(r, s) for r in roads for s in AUTOBAHN_SERVICES]
    failed = []

    def one(job):
        road, service = job
        try:
            return service, autobahn_get(f"/o/autobahn/{quote(road, safe='')}/services/{service}").get(service, [])
        except Exception:
            failed.append(job)
            return service, []

    rows, seen = [], set()
    with ThreadPoolExecutor(4) as ex:
        for service, items in ex.map(one, jobs):
            for it in items:
                c = it.get("coordinate") or {}
                lat, lon = float(c.get("lat", 0)), float(c.get("long", 0))
                if it.get("future") or it["identifier"] in seen or not in_bbox(lat, lon):
                    continue
                seen.add(it["identifier"])
                rows.append([AUTOBAHN_SERVICES[service], round(lat, 5), round(lon, 5), it.get("title", ""),
                             (it.get("subtitle") or "").strip(), text(it.get("description")),
                             it.get("isBlocked") == "true"])
    if len(failed) > len(jobs) // 2:
        raise OSError(f"{len(failed)} of {len(jobs)} Autobahn requests failed")
    return {"rows": rows, "failed": len(failed)}  # [kind, lat, lon, title, subtitle, description, blocked]


def simplify(coords, digits=3):
    """Round GeoJSON coordinates and drop the points that then repeat: NINA areas shrink ~5x."""
    if coords and isinstance(coords[0], (int, float)):
        return [round(coords[0], digits), round(coords[1], digits)]
    out = []
    for c in coords:
        c = simplify(c, digits)
        if not out or c != out[-1]:
            out.append(c)
    return out


def simplify_geometry(g):
    if g.get("type") == "GeometryCollection":
        return {"type": g["type"], "geometries": [simplify_geometry(x) for x in g.get("geometries", [])]}
    return {"type": g["type"], "coordinates": simplify(g.get("coordinates", []))}


_nina_cache = {}  # (id, version) -> feature; warnings rarely change, so only new versions are fetched


def nina():
    """Current civil-protection warnings (MoWaS, KATWARN, BIWAPP, flood centres, police) with their areas."""
    current = []
    for p in NINA_PROVIDERS:
        current += [(p, w) for w in get_json(f"{NINA}/{p}/mapData.json", timeout=30)]

    def one(pw):
        provider, w = pw
        key = (w["id"], w.get("version"))
        if key not in _nina_cache:
            info = get_json(f"{NINA}/warnings/{w['id']}.json", timeout=30)["info"][0]
            geo = get_json(f"{NINA}/warnings/{w['id']}.geojson", timeout=30)
            _nina_cache[key] = {
                "id": w["id"], "provider": provider, "severity": w.get("severity", ""),
                "start": epoch(w["startDate"]) if w.get("startDate") else None,
                "title": info.get("headline") or w.get("i18nTitle", {}).get("de", ""),
                "event": info.get("event", ""), "text": text(info.get("description")),
                "instruction": text(info.get("instruction"), 300),
                "areas": [simplify_geometry(f["geometry"]) for f in geo.get("features", []) if f.get("geometry")],
            }
        return _nina_cache[key]

    with ThreadPoolExecutor(6) as ex:
        features = [f for f in ex.map(lambda pw: _try(one, pw), current) if f]
    live = {(w["id"], w.get("version")) for _, w in current}
    for k in [k for k in _nina_cache if k not in live]:
        del _nina_cache[k]
    return {"warnings": features}


def _try(fn, arg):
    try:
        return fn(arg)
    except Exception:
        return None


def quakes():
    """Earthquakes of the last 30 days in and around Germany (EMSC)."""
    start = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 30 * 86400))
    data = get_json("https://www.seismicportal.eu/fdsnws/event/1/query", {
        "format": "json", "start": start, "limit": 1000, "orderby": "time",
        "minlat": BBOX[0] - 0.7, "maxlat": BBOX[2] + 0.5, "minlon": BBOX[1] - 0.8, "maxlon": BBOX[3] + 0.8}, timeout=30)
    rows = []
    for f in data.get("features", []):
        p = f["properties"]
        rows.append([round(p["lat"], 4), round(p["lon"], 4), p.get("mag"), abs(p.get("depth") or 0),
                     epoch(p["time"]), (p.get("flynn_region") or "").title()])
    return {"rows": rows}  # [lat, lon, magnitude, depth km, unix time, region]


BNETZA_MAP_APP = "https://www.arcgis.com/sharing/rest/content/items/cea067d3b63243d2a107bf406a329f53/data?f=json"


def coverage():
    """Date of the Bundesnetzagentur mobile coverage maps. The browser loads the map tiles itself; the date is only
    written in the official map app's text boxes ("Versorgungsdatenstand: 06.2026"), so take the most common one."""
    dates = Counter(re.findall(r"Versorgungsdatenstand\W*(\d{2}\.\d{4})", get(BNETZA_MAP_APP, timeout=60).decode()))
    return {"date": dates.most_common(1)[0][0] if dates else None}


LAYERS = {"flights": (flights, 10), "autobahn": (autobahn, 600), "nina": (nina, 300), "quakes": (quakes, 600),
          "coverage": (coverage, 86400)}


class Layers:
    """Serves each layer as a (JSON, gzipped JSON) pair, refreshing it in the background once it is older than its TTL."""

    def __init__(self):
        self.lock = threading.Lock()
        self.data, self.busy = {}, set()  # name -> (fetched at, (json, gzipped json))
        self.first = {name: threading.Lock() for name in LAYERS}

    def _refresh(self, name):
        try:
            body = dict(LAYERS[name][0](), fetched=int(time.time()))
        except Exception as e:
            body = {"error": str(e)[:200], "fetched": int(time.time())}
        raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        raw = (raw, gzip.compress(raw, 6))
        with self.lock:
            old = self.data.get(name)
            # keep the last good data if a refresh fails
            if "error" not in body or old is None:
                self.data[name] = (time.time(), raw)
            else:
                self.data[name] = (time.time(), old[1])
            self.busy.discard(name)

    def get(self, name):
        if name not in LAYERS:
            return None
        with self.lock:
            hit = self.data.get(name)
        if hit is None:
            with self.first[name]:  # several browsers asking at once: fetch once, the others wait for it
                if name not in self.data:
                    with self.lock:
                        self.busy.add(name)
                    self._refresh(name)
            return self.data[name][1]
        if time.time() - hit[0] > LAYERS[name][1]:
            with self.lock:
                start = name not in self.busy
                self.busy.add(name)
            if start:
                threading.Thread(target=self._refresh, args=(name,), daemon=True).start()
        return hit[1]


def main():
    for name, (fn, _) in LAYERS.items():
        t0 = time.time()
        try:
            d = fn()
            n = len(d.get("rows", d.get("warnings", [])))
            print(f"{name:10} {n:6} items  {len(json.dumps(d)) / 1e3:8.0f} kB  {time.time() - t0:5.1f} s")
        except Exception as e:
            print(f"{name:10} failed: {e}")


if __name__ == "__main__":
    main()
