#!/usr/bin/env python3
"""Collect public sensor data around Osnabrück, Georgsmarienhütte and Hagen a.T.W. into SQLite.

Usage:
    python3 collector.py            # fetch all sources once
    python3 collector.py summary    # show what is in the database

Only stdlib. Each source is independent: one failing source does not stop the others.
"""
import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
import math
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# From Quakenbrück (north of Landkreis Osnabrück) down to Essen and the Ruhr, Münsterland in between
BBOX = (51.38, 6.85, 52.80, 8.54)  # lat_min, lon_min, lat_max, lon_max
CENTER = ((BBOX[0] + BBOX[2]) / 2, (BBOX[1] + BBOX[3]) / 2)
RADIUS_KM = 100  # covers the BBOX corners; radius-based APIs are filtered back to the BBOX
DB_PATH = Path(__file__).with_name("osna.db")
UA = "osna-sensors/0.1 (private research collector)"

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    id TEXT PRIMARY KEY,          -- '<source>:<native id>'
    source TEXT NOT NULL,
    name TEXT,
    lat REAL, lon REAL,
    meta TEXT,                    -- JSON
    last_seen TEXT
);
CREATE TABLE IF NOT EXISTS readings (
    station_id TEXT NOT NULL REFERENCES stations(id),
    ts TEXT NOT NULL,             -- ISO 8601 UTC
    quantity TEXT NOT NULL,       -- normalized name, e.g. pm10, temperature
    value REAL,
    unit TEXT,
    PRIMARY KEY (station_id, ts, quantity)
);
CREATE INDEX IF NOT EXISTS readings_quantity_ts ON readings (quantity, ts);
CREATE INDEX IF NOT EXISTS readings_station_quantity_ts ON readings (station_id, quantity, ts);
CREATE TABLE IF NOT EXISTS runs (
    started TEXT, source TEXT, stations INTEGER, readings INTEGER, error TEXT
);
"""


def get_json(url, params=None, timeout=40):
    if params:
        url += "?" + urllib.parse.urlencode(params, safe=":,'")
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def utc(ts):
    """Normalize a timestamp string to ISO UTC without microseconds."""
    t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "T"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def dist_km(lat, lon):
    return math.hypot((lat - CENTER[0]) * 111.2, (lon - CENTER[1]) * 111.2 * math.cos(math.radians(lat)))


def in_bbox(lat, lon):
    return lat is not None and lon is not None and BBOX[0] <= lat <= BBOX[2] and BBOX[1] <= lon <= BBOX[3]


def fresh(ts, days=2):
    return ts and dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(utc(ts)) < dt.timedelta(days=days)


# Each source yields (station, readings): station = dict(id, name, lat, lon, meta),
# readings = list of (ts, quantity, value, unit).

SC_QUANTITIES = {
    "P1": ("pm10", "µg/m³", 1), "P2": ("pm2_5", "µg/m³", 1), "P0": ("pm1", "µg/m³", 1),
    "temperature": ("temperature", "°C", 1), "humidity": ("humidity", "%", 1),
    "pressure": ("pressure", "hPa", 0.01), "pressure_at_sealevel": ("pressure_msl", "hPa", 0.01),
    "noise_LAeq": ("noise_laeq", "dB(A)", 1),
}


def sensor_community():
    data = get_json("https://data.sensor.community/airrohr/v1/filter/box=" + ",".join(map(str, BBOX)))
    for e in data:
        s, loc = e["sensor"], e["location"]
        if loc.get("indoor"):
            continue
        readings = []
        for v in e["sensordatavalues"]:
            if v["value_type"] in SC_QUANTITIES:
                q, unit, f = SC_QUANTITIES[v["value_type"]]
                try:
                    readings.append((utc(e["timestamp"]), q, float(v["value"]) * f, unit))
                except ValueError:
                    pass
        yield ({"id": f"sc:{s['id']}", "name": f"{s['sensor_type']['name']} #{s['id']}",
                "lat": float(loc["latitude"]), "lon": float(loc["longitude"]),
                "meta": {"sensor_type": s["sensor_type"]["name"], "location_id": loc["id"],
                         "indoor": loc.get("indoor")}}, readings)


OSM_QUANTITIES = {  # openSenseMap titles are free text; map the common ones
    "temperatur": "temperature", "rel. luftfeuchte": "humidity", "luftfeuchte": "humidity",
    "luftdruck": "pressure", "pm10": "pm10", "pm2.5": "pm2_5", "pm1.0": "pm1", "pm4.0": "pm4",
    "uv-intensität": "uv_intensity", "beleuchtungsstärke": "illuminance", "co2": "co2", "co₂": "co2",
    "windgeschwindigkeit": "wind_speed", "windrichtung": "wind_direction", "lautstärke": "noise",
    "bodenfeuchte": "soil_moisture", "bodentemperatur": "soil_temperature",
}


def osm_quantity(title):
    t = title.strip().lower()
    for key, q in OSM_QUANTITIES.items():
        if t.startswith(key):
            depth = t[len(key):].strip().replace(" ", "")
            return f"{q}_{depth}" if depth else q
    return "osm_" + "".join(c if c.isalnum() else "_" for c in t).strip("_")


def opensensemap():
    lat0, lon0, lat1, lon1 = BBOX
    boxes = get_json("https://api.opensensemap.org/boxes",
                     {"bbox": f"{lon0},{lat0},{lon1},{lat1}", "full": "true"}, timeout=90)
    for b in boxes:
        if not fresh(b.get("lastMeasurementAt")) or b.get("exposure") == "indoor":
            continue
        lon, lat = b["currentLocation"]["coordinates"][:2]
        readings = []
        for s in b.get("sensors", []):
            m = s.get("lastMeasurement")
            if m and fresh(m.get("createdAt")):
                try:
                    readings.append((utc(m["createdAt"]), osm_quantity(s["title"]), float(m["value"]), s.get("unit")))
                except (TypeError, ValueError):
                    pass
        yield ({"id": f"osm:{b['_id']}", "name": b["name"], "lat": lat, "lon": lon,
                "meta": {"exposure": b.get("exposure"), "model": b.get("model"),
                         "sensors": [s["title"] for s in b.get("sensors", [])]}}, readings)


DWD_FIELDS = {
    "temperature": "°C", "dew_point": "°C", "relative_humidity": "%", "pressure_msl": "hPa",
    "precipitation_10": "mm", "precipitation_60": "mm", "solar_10": "kWh/m²", "solar_60": "kWh/m²",
    "sunshine_30": "min", "sunshine_60": "min", "wind_speed_10": "km/h", "wind_direction_10": "°",
    "wind_gust_speed_10": "km/h", "cloud_cover": "%", "visibility": "m",
}


def dwd_brightsky():
    sources = get_json("https://api.brightsky.dev/sources",
                       {"lat": CENTER[0], "lon": CENTER[1], "max_dist": RADIUS_KM * 1000})["sources"]
    sources = [s for s in sources if s["observation_type"] in ("current", "synop")
               and fresh(s["last_record"], 1) and in_bbox(s["lat"], s["lon"])]

    def current(src):
        for attempt in range(2):
            try:
                return get_json("https://api.brightsky.dev/current_weather", {"source_id": src["id"]})["weather"]
            except urllib.error.HTTPError:  # some synop sources have no current record
                return None
            except (urllib.error.URLError, ConnectionError, TimeoutError):  # occasional resets under load
                time.sleep(2)
        return None

    with ThreadPoolExecutor(4) as pool:  # one request per station; ~50 stations in the area
        weathers = list(pool.map(current, sources))
    for src, w in zip(sources, weathers):
        if w is None:
            continue
        fallback = w.get("fallback_source_ids") or {}  # fields filled from other stations
        readings = [(utc(w["timestamp"]), k, float(w[k]), u)
                    for k, u in DWD_FIELDS.items() if w.get(k) is not None and k not in fallback]
        yield ({"id": f"dwd:{src['id']}", "name": src["station_name"], "lat": src["lat"], "lon": src["lon"],
                "meta": {"dwd_station_id": src.get("dwd_station_id"), "wmo_station_id": src.get("wmo_station_id"),
                         "observation_type": src["observation_type"]}}, readings)


PEGEL_QUANTITIES = {"W": "water_level", "WT": "water_temperature", "LT": "air_temperature",
                    "WG": "flow_speed", "WR": "flow_direction", "Q": "discharge", "LF": "conductivity",
                    "O2": "oxygen", "PH": "ph"}


def pegelonline():
    stations = get_json("https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json",
                        {"latitude": CENTER[0], "longitude": CENTER[1], "radius": RADIUS_KM,
                         "includeTimeseries": "true", "includeCurrentMeasurement": "true"})
    for s in stations:
        if not in_bbox(s.get("latitude"), s.get("longitude")):
            continue
        readings = []
        for t in s["timeseries"]:
            m = t.get("currentMeasurement")
            if m and m.get("value") is not None:
                q = PEGEL_QUANTITIES.get(t["shortname"], "pegel_" + t["shortname"].lower())
                readings.append((utc(m["timestamp"]), q, float(m["value"]), t.get("unit")))
        yield ({"id": f"pegel:{s['uuid']}", "name": f"{s['shortname']} ({s['water']['shortname']})",
                "lat": s.get("latitude"), "lon": s.get("longitude"),
                "meta": {"number": s.get("number"), "water": s["water"]["longname"]}}, readings)


def bfs_odl():
    lat0, lon0, lat1, lon1 = BBOX
    fc = get_json("https://www.imis.bfs.de/ogc/opendata/ows",
                  {"service": "WFS", "version": "1.1.0", "request": "GetFeature",
                   "typeName": "opendata:odlinfo_odl_1h_latest", "outputFormat": "application/json",
                   "bbox": f"{lat0},{lon0},{lat1},{lon1},urn:ogc:def:crs:EPSG::4326"})
    for f in fc["features"]:
        p = f["properties"]
        lon, lat = f["geometry"]["coordinates"]
        readings = []
        if p.get("value") is not None:
            ts = utc(p["end_measure"])
            readings += [(ts, "gamma_dose_rate", p["value"], p["unit"])]
            if p.get("value_terrestrial") is not None:
                readings += [(ts, "gamma_dose_rate_terrestrial", p["value_terrestrial"], p["unit"])]
        yield ({"id": f"odl:{p['kenn']}", "name": p["name"], "lat": lat, "lon": lon,
                "meta": {"status": p.get("site_status_text"), "height": p.get("height_above_sea")}}, readings)


def bfs_uv():
    lat0, lon0, lat1, lon1 = BBOX
    since = (dt.date.today() - dt.timedelta(days=7)).isoformat()
    fc = get_json("https://www.imis.bfs.de/ogc/opendata/ows",
                  {"service": "WFS", "version": "1.1.0", "request": "GetFeature",
                   "typeName": "opendata:uv_index", "outputFormat": "application/json",
                   "CQL_FILTER": f"BBOX(geom,{lat0},{lon0},{lat1},{lon1}) AND date >= '{since}'"})  # lat/lon order
    by_station = {}
    for f in fc["features"]:
        by_station.setdefault(f["properties"]["station_id"], []).append(f)
    for sid, feats in by_station.items():
        p0 = feats[0]["properties"]
        lon, lat = feats[0]["geometry"]["coordinates"]
        readings = [(utc(f["properties"]["date"]), "uv_index_daily_max", float(f["properties"]["uv_index"]), "UVI")
                    for f in feats if f["properties"].get("uv_index") is not None]
        yield ({"id": f"uv:{sid}", "name": p0["station_name"], "lat": lat, "lon": lon,
                "meta": {"graph": p0.get("tagesgang_url")}}, readings)


UBA = "https://luftdaten.umweltbundesamt.de/api/air-data/v4"
UBA_COMPONENTS = {"1": "pm10", "3": "o3", "5": "no2", "9": "pm2_5"}


def uba():
    # One request per component returns all German stations; we keep the active ones in the BBOX.
    # The measures endpoint is flaky, so a failing component is skipped and the others still count.
    st = get_json(f"{UBA}/stations/json", {"lang": "de"})
    stations = {sid: v for sid, v in st["data"].items()
                if not v[6] and in_bbox(float(v[8]), float(v[7]))}  # v[6] = active to, v[7]/v[8] = lon/lat
    readings = {sid: [] for sid in stations}
    today = dt.date.today()
    failed = []
    for cid, q in UBA_COMPONENTS.items():
        try:
            d = get_json(f"{UBA}/measures/json",
                         {"component": cid, "scope": "2", "date_from": (today - dt.timedelta(days=1)).isoformat(),
                          "time_from": "1", "date_to": today.isoformat(), "time_to": "24"}, timeout=90)
        except Exception:
            failed.append(q)
            continue
        for sid, rows in d.get("data", {}).items():
            if sid in readings:
                readings[sid] += [(utc(start + "+01:00"), q, float(row[2]), "µg/m³")
                                  for start, row in rows.items() if row[2] is not None]
    if len(failed) == len(UBA_COMPONENTS):
        raise RuntimeError("UBA measures endpoint failed for all components")
    for sid, v in stations.items():
        if readings[sid]:
            yield ({"id": f"uba:{v[1]}", "name": v[2], "lat": float(v[8]), "lon": float(v[7]),
                    "meta": {"uba_id": sid, "city": v[3], "network": v[13]}}, readings[sid])


POLLEN_LEVEL = {"0": 0, "0-1": 0.5, "1": 1, "1-2": 1.5, "2": 2, "2-3": 2.5, "3": 3}


def dwd_pollen():
    d = get_json("https://opendata.dwd.de/climate_environment/health/alerts/s31fg.json")
    region = next(c for c in d["content"] if c["partregion_id"] == 31)  # Westl. Niedersachsen/Bremen
    day = dt.datetime.strptime(d["last_update"][:10], "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    readings = [(day.isoformat(), f"pollen_{name.lower()}", POLLEN_LEVEL[v["today"]], "level 0-3")
                for name, v in region["Pollen"].items() if v["today"] in POLLEN_LEVEL]
    yield ({"id": "pollen:31", "name": region["partregion_name"], "lat": None, "lon": None,
            "meta": {"region": region["region_name"], "forecast": "today"}}, readings)


SOURCES = {"sensor_community": sensor_community, "opensensemap": opensensemap, "dwd": dwd_brightsky,
           "pegelonline": pegelonline, "bfs_odl": bfs_odl, "bfs_uv": bfs_uv, "uba": uba,
           "dwd_pollen": dwd_pollen}


def collect(db):
    started = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    for name, fn in SOURCES.items():
        seen, n_rd = set(), 0
        err = None
        try:
            for st, readings in fn():  # a station may appear more than once (e.g. sensor.community)
                db.execute("""INSERT INTO stations (id, source, name, lat, lon, meta, last_seen)
                              VALUES (?,?,?,?,?,?,?)
                              ON CONFLICT(id) DO UPDATE SET name=excluded.name, lat=excluded.lat,
                                lon=excluded.lon, meta=excluded.meta, last_seen=excluded.last_seen""",
                           (st["id"], name, st["name"], st["lat"], st["lon"],
                            json.dumps(st["meta"], ensure_ascii=False), started))
                cur = db.executemany("INSERT OR IGNORE INTO readings VALUES (?,?,?,?,?)",
                                     [(st["id"], *r) for r in readings])
                seen.add(st["id"])
                n_rd += cur.rowcount
        except Exception as e:  # keep other sources running
            err = f"{type(e).__name__}: {e}"
        db.execute("INSERT INTO runs VALUES (?,?,?,?,?)", (started, name, len(seen), n_rd, err))
        db.commit()
        print(f"{name:17} {len(seen):4} stations {n_rd:5} new readings" + (f"  ERROR {err}" if err else ""))


def summary(db):
    print("Stations seen in the last 24 h, by source:")
    for row in db.execute("""SELECT source, COUNT(*) FROM stations
                             WHERE last_seen > datetime('now','-1 day') GROUP BY source ORDER BY 2 DESC"""):
        print(f"  {row[0]:17} {row[1]}")
    print("Distinct station × quantity series:",
          db.execute("SELECT COUNT(*) FROM (SELECT DISTINCT station_id, quantity FROM readings)").fetchone()[0])
    print("Readings total:", db.execute("SELECT COUNT(*) FROM readings").fetchone()[0])
    print("Top quantities:")
    for q, n in db.execute("""SELECT quantity, COUNT(DISTINCT station_id) FROM readings
                              GROUP BY quantity ORDER BY 2 DESC LIMIT 15"""):
        print(f"  {q:28} {n} stations")


def main():
    db = sqlite3.connect(DB_PATH)
    db.executescript(SCHEMA)
    summary(db) if sys.argv[1:] == ["summary"] else collect(db)


if __name__ == "__main__":
    main()
