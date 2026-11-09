#!/usr/bin/env python3
"""Collect public sensor data for all of Germany into SQLite.

Usage:
    python3 collector.py            # fetch all sources once
    python3 collector.py summary    # show what is in the database

Only stdlib. Sources are fetched in parallel; one failing source does not stop the others.
Storage is tuned for small machines: integer keys, one table of raw readings kept for RAW_DAYS,
hourly aggregates kept for HOURLY_DAYS, and a `latest` table so the map never scans history.
"""
import datetime as dt
import gzip
import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BBOX = (47.2, 5.8, 55.1, 15.1)  # Germany: lat_min, lon_min, lat_max, lon_max
DB_PATH = Path(__file__).with_name("germasense.db")
UA = "germasense/0.2 (+https://github.com/ishallnotforget)"
RAW_DAYS = 2       # full-resolution readings (charts up to 48 h)
HOURLY_DAYS = 14   # hourly averages (7-day charts)

SCHEMA = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,     -- '<prefix>:<native id>'
    source TEXT NOT NULL,
    name TEXT,
    lat REAL, lon REAL,
    meta TEXT,                    -- JSON
    last_seen INTEGER             -- unix time
);
CREATE TABLE IF NOT EXISTS quantities (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, unit TEXT);
CREATE TABLE IF NOT EXISTS readings (
    sid INTEGER NOT NULL, qid INTEGER NOT NULL, ts INTEGER NOT NULL, value REAL,
    PRIMARY KEY (sid, qid, ts)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS hourly (
    sid INTEGER NOT NULL, qid INTEGER NOT NULL, hour INTEGER NOT NULL,
    sum REAL, n INTEGER, min REAL, max REAL,
    PRIMARY KEY (sid, qid, hour)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS latest (
    qid INTEGER NOT NULL, sid INTEGER NOT NULL, ts INTEGER NOT NULL, value REAL,
    PRIMARY KEY (qid, sid)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS runs (
    started INTEGER, source TEXT, stations INTEGER, readings INTEGER, error TEXT, seconds REAL
);
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
"""


def connect(path=DB_PATH):
    db = sqlite3.connect(path, timeout=60, check_same_thread=False)
    db.executescript(SCHEMA)
    db.execute("PRAGMA synchronous = NORMAL")
    db.execute("PRAGMA cache_size = -32000")  # 32 MB
    if not db.execute("SELECT 1 FROM kv WHERE key = 'quantities_v2'").fetchone():
        merge_quantities(db)
    return db


def merge_quantities(db):
    """One-time migration for databases from before ALIASES/osm_quantity: move each old quantity's readings to its
    shared name (converting units), and drop the ones that aren't environmental measurements."""
    sources = {}
    for qid, source in db.execute("SELECT DISTINCT l.qid, s.source FROM latest l JOIN stations s ON s.id = l.sid"):
        sources.setdefault(qid, set()).add(source)
    ids = dict(db.execute("SELECT name, id FROM quantities"))
    for qid, name, unit in db.execute("SELECT id, name, unit FROM quantities").fetchall():
        src = sources.get(qid, set())
        if name in ALIASES:
            target = (ALIASES[name], 1)
        elif name.startswith("pegel_"):
            target = None
        elif src == {"opensensemap"}:
            target = osm_quantity(name[4:] if name.startswith("osm_") else name, unit)
        elif name in UNITS:
            target = (name, UNIT_FACTORS.get(name, {}).get(unit_key(unit)) or 1)
        else:
            continue
        if target == (name, 1) and (name not in UNITS or unit == UNITS[name]):
            continue
        series = [r[0] for r in db.execute("SELECT sid FROM latest WHERE qid = ?", (qid,))]
        if target and target[0] == name:  # same name, other unit: convert in place
            f = target[1]
            for sid in series:
                db.execute("UPDATE readings SET value = value * ? WHERE sid = ? AND qid = ?", (f, sid, qid))
                db.execute("UPDATE hourly SET sum = sum * ?, min = min * ?, max = max * ? WHERE sid = ? AND qid = ?",
                           (f, f, f, sid, qid))
            db.execute("UPDATE latest SET value = value * ? WHERE qid = ?", (f, qid))
            db.execute("UPDATE quantities SET unit = ? WHERE id = ?", (UNITS[name], qid))
            continue
        if target:
            new, f = target
            if new not in ids:
                ids[new] = db.execute("INSERT INTO quantities (name, unit) VALUES (?,?)",
                                      (new, UNITS.get(new, unit))).lastrowid
            tid = ids[new]
            for sid in series:  # per series, so every statement uses the primary key
                db.execute("INSERT OR IGNORE INTO readings SELECT sid, ?, ts, value * ? FROM readings WHERE sid = ? AND qid = ?",
                           (tid, f, sid, qid))
                db.execute("""INSERT OR IGNORE INTO hourly SELECT sid, ?, hour, sum * ?, n, min * ?, max * ?
                              FROM hourly WHERE sid = ? AND qid = ?""", (tid, f, f, f, sid, qid))
            db.execute("""INSERT INTO latest SELECT ?, sid, ts, value * ? FROM latest WHERE qid = ? AND true
                          ON CONFLICT (qid, sid) DO UPDATE SET ts = excluded.ts, value = excluded.value
                          WHERE excluded.ts > latest.ts""", (tid, f, qid))
        for sid in series:
            db.execute("DELETE FROM readings WHERE sid = ? AND qid = ?", (sid, qid))
            db.execute("DELETE FROM hourly WHERE sid = ? AND qid = ?", (sid, qid))
        db.execute("DELETE FROM latest WHERE qid = ?", (qid,))
        db.execute("DELETE FROM quantities WHERE id = ?", (qid,))
    db.execute("INSERT OR REPLACE INTO kv VALUES ('quantities_v2', '1')")
    db.commit()


def get(url, params=None, timeout=60):
    if params:
        url += "?" + urllib.parse.urlencode(params, safe=":,'")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        return gzip.decompress(data) if r.headers.get("Content-Encoding") == "gzip" else data


def get_json(url, params=None, timeout=60):
    return json.loads(get(url, params, timeout))


def epoch(ts):
    """Timestamp string (ISO, 'Z', space separator or naive UTC) -> unix seconds."""
    # Python 3.9's fromisoformat only takes 3 or 6 fraction digits ("08:06:06.4" fails), and we keep whole seconds anyway
    t = dt.datetime.fromisoformat(re.sub(r"\.\d+", "", ts.replace("Z", "+00:00").replace(" ", "T")))
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return int(t.timestamp())


def in_bbox(lat, lon):
    return lat is not None and lon is not None and BBOX[0] <= lat <= BBOX[2] and BBOX[1] <= lon <= BBOX[3]


def fresh(ts, days=2):
    return bool(ts) and time.time() - epoch(ts) < days * 86400


# Each source yields (station, readings): station = dict(key, name, lat, lon, meta),
# readings = list of (unix ts, quantity, value, unit).

SC_QUANTITIES = {
    "P1": ("pm10", "µg/m³", 1), "P2": ("pm2_5", "µg/m³", 1), "P0": ("pm1", "µg/m³", 1),
    "temperature": ("temperature", "°C", 1), "humidity": ("humidity", "%", 1),
    "pressure": ("pressure", "hPa", 0.01), "pressure_at_sealevel": ("pressure_msl", "hPa", 0.01),
    "noise_LAeq": ("noise_laeq", "dB(A)", 1),
}


def sensor_community():
    data = get_json("https://data.sensor.community/airrohr/v1/filter/country=DE", timeout=120)
    for e in data:
        s, loc = e["sensor"], e["location"]
        if loc.get("indoor"):
            continue
        ts = epoch(e["timestamp"])
        readings = []
        for v in e["sensordatavalues"]:
            if v["value_type"] in SC_QUANTITIES:
                q, unit, f = SC_QUANTITIES[v["value_type"]]
                try:
                    readings.append((ts, q, float(v["value"]) * f, unit))
                except ValueError:
                    pass
        yield ({"key": f"sc:{s['id']}", "name": f"{s['sensor_type']['name']} #{s['id']}",
                "lat": float(loc["latitude"]), "lon": float(loc["longitude"]),
                "meta": {"sensor_type": s["sensor_type"]["name"], "location_id": loc["id"]}}, readings)


# One name per measured thing, whatever the source calls it, so the map shows e.g. all temperature sensors together.
ALIASES = {"relative_humidity": "humidity", "air_temperature": "temperature", "noise_laeq": "noise",
           "wind_speed_10": "wind_speed", "wind_direction_10": "wind_direction", "wind_gust_speed_10": "wind_gust"}
UNIT_FACTORS = {  # quantity -> {unit as sources write it (lowercase, no spaces): factor to the first unit}
    "wind_speed": {"km/h": 1, "m/s": 3.6, "m*s^-1": 3.6, "kmh": 1, "kn": 1.852, "mph": 1.609},
    "pressure": {"hpa": 1, "mbar": 1, "pa": 0.01, "kpa": 10},
}
UNIT_FACTORS["wind_gust"] = UNIT_FACTORS["wind_speed"]
UNIT_FACTORS["pressure_msl"] = UNIT_FACTORS["pressure"]
UNITS = {"wind_speed": "km/h", "wind_gust": "km/h", "pressure": "hPa", "pressure_msl": "hPa", "noise": "dB(A)"}


def unit_key(unit):
    return (unit or "").lower().replace(" ", "").replace("μ", "µ")


def osm_quantity(title, unit):
    """openSenseMap sensor titles are free text in many languages ("Rel. Luftfeuchte", "humidity (BME280)", "PM2.5").
    Returns (quantity, factor to its unit), or None for channels that aren't measurements of the environment
    (battery, Wi-Fi, GPS, ...) or whose unit doesn't fit."""
    t, u = title.lower().replace("_", " "), unit_key(unit)
    has = lambda *words: any(w in t for w in words)
    if has("batter", "volt", "spannung", "rssi", "signal", "wifi", "latitude", "longitude", "longtitude", "timestamp",
           "energy", "power", "current", "frequen", "capacity", "gefühlt", "heat index", "spread", "differenz", "vpd"):
        return None
    celsius = u in ("°c", "c", "celsius", "°", "grad", "gradcelsius")
    if has("taupunkt", "dew"):
        return ("dew_point", 1) if celsius else None
    if has("boden", "soil"):
        if has("temp"):
            return ("soil_temperature", 1) if celsius else None
        return ("soil_moisture", 1) if has("feucht", "moist") and "%" in u else None
    if has("wasser", "water"):
        return ("water_temperature", 1) if has("temp") and celsius else None
    if has("temp"):
        return ("temperature", 1) if celsius else None
    if has("feucht", "humid"):
        return ("humidity", 1) if "%" in u or "rh" in u else None
    if has("druck", "pressure") and not has("sound", "schall"):
        q = "pressure_msl" if re.search(r"\brel\b|relativ|\bnn\b|meeres|msl|sea", t) else "pressure"
        f = UNIT_FACTORS[q].get(u)
        return (q, f) if f else None
    m = re.search(r"pm\s*(\d+)(?:[.,\s](\d))?", t)
    if m:
        size = {"10": "pm10", "25": "pm2_5", "2": "pm2_5", "1": "pm1", "01": "pm1", "4": "pm4"}.get(m.group(1))
        if m.group(1) == "1" and m.group(2) and m.group(2) != "0":
            size = None
        return (size, 1) if size and "g/m" in u else None
    if has("uv"):
        if u in ("uvi", "index") or has("index"):
            return "uv_index", 1
        return ("uv_intensity", 1) if "w/cm" in u else None
    if "lx" in u or "lux" in u:
        return "illuminance", 1
    if "w/m" in u and has("solar", "sonne", "strahlung", "radiation", "global"):
        return "solar_radiation", 1
    if has("wind"):
        if has("richtung", "direction"):
            return ("wind_direction", 1) if u in ("°", "grad", "deg", "degree") else None
        q = "wind_gust" if has("böe", "gust") else "wind_speed"
        f = UNIT_FACTORS[q].get(u)
        return (q, f) if f else None
    if has("böe", "gust"):
        f = UNIT_FACTORS["wind_gust"].get(u)
        return ("wind_gust", f) if f else None
    if has("lautst", "noise", "geräusch", "schall", "sound", "lärm") and u.startswith("db"):
        return "noise", 1
    if has("co2", "co₂") and u == "ppm":
        return "co2", 1
    if u == "µsv/h":
        return "gamma_dose_rate", 1
    if has("regen", "rain", "niederschlag") and u == "mm/h":
        return "rain_rate", 1
    return None


def opensensemap():
    lat0, lon0, lat1, lon1 = BBOX
    boxes = get_json("https://api.opensensemap.org/boxes",
                     {"bbox": f"{lon0},{lat0},{lon1},{lat1}", "full": "true"}, timeout=300)
    for b in boxes:
        if not fresh(b.get("lastMeasurementAt")) or b.get("exposure") == "indoor":
            continue
        lon, lat = b["currentLocation"]["coordinates"][:2]
        readings = []
        for s in b.get("sensors", []):
            m = s.get("lastMeasurement")
            if m and fresh(m.get("createdAt")):
                q = osm_quantity(s.get("title") or "", s.get("unit"))
                if not q:
                    continue
                try:
                    readings.append((epoch(m["createdAt"]), q[0], float(m["value"]) * q[1], s.get("unit")))
                except (TypeError, ValueError):
                    pass
        yield ({"key": f"osm:{b['_id']}", "name": b["name"], "lat": lat, "lon": lon,
                "meta": {"exposure": b.get("exposure"), "model": b.get("model")}}, readings)


# DWD publishes all ~1000 German stations every 10 minutes as SYNOP/BUFR batches converted to GeoJSON.
# The values come as a flat BUFR sequence where "height of sensor" and "time period" set the context
# for the values after them, so we walk the sequence and keep that context.
DWD_GEOJSON = "https://opendata.dwd.de/weather/weather_reports/synoptic/germany/geojson/"
DWD_LOOKBACK_S = 40 * 60  # new batch files appear every few minutes; 40 min covers every station


def dwd_report(seq):
    out, height, period = {}, None, None

    def put(q, v, unit):
        if q not in out:
            out[q] = (v, unit)

    for name, v in seq:
        if name == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform":
            height = v
        elif name == "timePeriod":
            period = v
        elif v is None:
            continue
        elif name == "airTemperature" and height is not None:
            if 1 <= height <= 10:  # 2 m standard; mountain stations measure higher
                put("temperature", v - 273.15, "°C")
            elif height < 0.2:
                put("ground_temperature", v - 273.15, "°C")
        elif name == "dewpointTemperature":
            put("dew_point", v - 273.15, "°C")
        elif name == "relativeHumidity":
            put("relative_humidity", v, "%")
        elif name == "pressureReducedToMeanSeaLevel":
            put("pressure_msl", v / 100, "hPa")
        elif name == "windSpeed":
            put("wind_speed_10", v * 3.6, "km/h")
        elif name == "windDirection":
            put("wind_direction_10", v, "°")
        elif name == "maximumWindGustSpeed":
            put("wind_gust_speed_10", v * 3.6, "km/h")
        elif name == "cloudCoverTotal":
            put("cloud_cover", v, "%")
        elif name == "MeteorologicalOpticalRange":
            put("visibility", v, "m")
        elif name == "totalSnowDepth" and v >= 0:
            put("snow_depth", v * 100, "cm")
        elif name == "totalPrecipitationOrTotalWaterEquivalent" and period == -10:
            put("precipitation_10", v, "mm")
        elif name == "sunshineOverPeriodSpecified" and period in (-10, -60):
            put(f"sunshine_{-period}", v, "min")
        elif name == "globalSolarRadiationIntegratedOverPeriodSpecified" and period and period < 0:
            put("solar_radiation", v / (-period * 60), "W/m²")  # J/m² over the period -> mean W/m²
    return out


def dwd():
    listing = get(DWD_GEOJSON).decode()
    now = time.time()
    files = []
    for name, stamp in sorted(set(re.findall(r'href="(Z__C_EDZW_(\d{14})[^"]*\.geojson\.gz)"', listing))):
        t = dt.datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc).timestamp()
        if now - t < DWD_LOOKBACK_S:
            files.append(name)
    with ThreadPoolExecutor(4) as pool:
        batches = list(pool.map(lambda f: json.loads(gzip.decompress(get(DWD_GEOJSON + f))), files))
    stations = {}
    for fc in batches:
        for f in fc["features"]:
            p = f["properties"]
            seq = [(v["shortname"], v["value"]) for k, v in sorted(p.items()) if k.startswith("data_")]
            d = dict(seq)
            sid = str(d.get("wigosLocalIdentifierCharacter") or "").strip()
            if not sid or d.get("minute") is None:
                continue
            ts = int(dt.datetime(d["year"], d["month"], d["day"], d["hour"], d["minute"],
                                 tzinfo=dt.timezone.utc).timestamp())
            lon, lat = f["geometry"]["coordinates"][:2]
            st = stations.setdefault(sid, ({
                "key": f"dwd:{sid}", "name": str(d.get("stationOrSiteName") or sid).strip(), "lat": lat, "lon": lon,
                "meta": {"wigos_id": sid, "type": d.get("stationType"),
                         "height": d.get("heightOfBarometerAboveMeanSeaLevel")}}, {}))
            for q, (v, unit) in dwd_report(seq).items():
                st[1][(ts, q)] = (ts, q, v, unit)
    for station, readings in stations.values():
        yield station, list(readings.values())


PEGEL_QUANTITIES = {"W": "water_level", "WT": "water_temperature", "LT": "air_temperature",
                    "WG": "flow_speed", "WR": "flow_direction", "Q": "discharge", "LF": "conductivity",
                    "O2": "oxygen", "PH": "ph"}


def pegelonline():
    stations = get_json("https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json",
                        {"includeTimeseries": "true", "includeCurrentMeasurement": "true"})
    for s in stations:
        if s.get("latitude") is None:
            continue
        readings = []
        for t in s["timeseries"]:
            m = t.get("currentMeasurement")
            if m and m.get("value") is not None:
                q = PEGEL_QUANTITIES.get(t["shortname"])  # rare special gauges (turbidity, ...) are left out
                readings.append((epoch(m["timestamp"]), q, float(m["value"]), t.get("unit")))
        yield ({"key": f"pegel:{s['uuid']}", "name": f"{s['shortname']} ({s['water']['shortname']})",
                "lat": s["latitude"], "lon": s["longitude"],
                "meta": {"number": s.get("number"), "water": s["water"]["longname"]}}, readings)


BFS_WFS = "https://www.imis.bfs.de/ogc/opendata/ows"


def bfs_odl():
    fc = get_json(BFS_WFS, {"service": "WFS", "version": "1.1.0", "request": "GetFeature",
                            "typeName": "opendata:odlinfo_odl_1h_latest", "outputFormat": "application/json"})
    for f in fc["features"]:
        p = f["properties"]
        lon, lat = f["geometry"]["coordinates"]
        readings = []
        if p.get("value") is not None:
            ts = epoch(p["end_measure"])
            readings += [(ts, "gamma_dose_rate", p["value"], p["unit"])]
            if p.get("value_terrestrial") is not None:
                readings += [(ts, "gamma_dose_rate_terrestrial", p["value_terrestrial"], p["unit"])]
        yield ({"key": f"odl:{p['kenn']}", "name": p["name"], "lat": lat, "lon": lon,
                "meta": {"status": p.get("site_status_text"), "height": p.get("height_above_sea")}}, readings)


def bfs_uv():
    since = (dt.date.today() - dt.timedelta(days=7)).isoformat()
    fc = get_json(BFS_WFS, {"service": "WFS", "version": "1.1.0", "request": "GetFeature",
                            "typeName": "opendata:uv_index", "outputFormat": "application/json",
                            "CQL_FILTER": f"date >= '{since}'"})
    by_station = {}
    for f in fc["features"]:
        by_station.setdefault(f["properties"]["station_id"], []).append(f)
    for sid, feats in by_station.items():
        p0 = feats[0]["properties"]
        lon, lat = feats[0]["geometry"]["coordinates"]
        readings = [(epoch(f["properties"]["date"]), "uv_index_daily_max", float(f["properties"]["uv_index"]), "UVI")
                    for f in feats if f["properties"].get("uv_index") is not None]
        yield ({"key": f"uv:{sid}", "name": p0["station_name"], "lat": lat, "lon": lon,
                "meta": {"graph": p0.get("tagesgang_url")}}, readings)


UBA = "https://luftdaten.umweltbundesamt.de/api/air-data/v4"
UBA_COMPONENTS = {"1": ("pm10", "µg/m³"), "2": ("co", "mg/m³"), "3": ("o3", "µg/m³"),
                  "4": ("so2", "µg/m³"), "5": ("no2", "µg/m³"), "9": ("pm2_5", "µg/m³")}


def uba():
    # One request per component returns all German stations. The measures endpoint is flaky,
    # so a failing component is skipped and the others still count.
    st = get_json(f"{UBA}/stations/json", {"lang": "de"})
    stations = {sid: v for sid, v in st["data"].items() if not v[6]}  # v[6] = active until (empty = active)
    readings = {sid: [] for sid in stations}
    today = dt.date.today()
    failed = []
    for cid, (q, unit) in UBA_COMPONENTS.items():
        try:
            d = get_json(f"{UBA}/measures/json",
                         {"component": cid, "scope": "2", "date_from": (today - dt.timedelta(days=1)).isoformat(),
                          "time_from": "1", "date_to": today.isoformat(), "time_to": "24"}, timeout=120)
        except Exception:
            failed.append(q)
            continue
        for sid, rows in d.get("data", {}).items():
            if sid in readings:
                readings[sid] += [(epoch(start + "+01:00"), q, float(row[2]), unit)
                                  for start, row in rows.items() if row[2] is not None]
    if len(failed) == len(UBA_COMPONENTS):
        raise RuntimeError("UBA measures endpoint failed for all components")
    for sid, v in stations.items():
        if readings[sid]:
            yield ({"key": f"uba:{v[1]}", "name": v[2], "lat": float(v[8]), "lon": float(v[7]),
                    "meta": {"uba_id": sid, "city": v[3], "network": v[13]}}, readings[sid])


POLLEN_LEVEL = {"0": 0, "0-1": 0.5, "1": 1, "1-2": 1.5, "2": 2, "2-3": 2.5, "3": 3}


def dwd_pollen():
    d = get_json("https://opendata.dwd.de/climate_environment/health/alerts/s31fg.json")
    day = int(dt.datetime.strptime(d["last_update"][:10], "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())
    for region in d["content"]:
        rid = region["partregion_id"] if region["partregion_id"] != -1 else region["region_id"]
        readings = [(day, f"pollen_{name.lower()}", POLLEN_LEVEL[v["today"]], "level 0-3")
                    for name, v in region["Pollen"].items() if v["today"] in POLLEN_LEVEL]
        yield ({"key": f"pollen:{rid}", "name": region["partregion_name"] or region["region_name"],
                "lat": None, "lon": None, "meta": {"region": region["region_name"], "forecast": "today"}}, readings)


PARKENDD = "https://api.parkendd.de/"


def parking():
    cities = [c for c, info in get_json(PARKENDD)["cities"].items() if info.get("active_support")]

    def fetch(city):
        try:
            return get_json(PARKENDD + urllib.parse.quote(city), timeout=30)
        except Exception:
            return None  # one city's scraper being down should not drop the others

    with ThreadPoolExecutor(6) as pool:
        results = list(pool.map(fetch, cities))
    for city, d in zip(cities, results):
        if d is None:
            continue
        ts = epoch(d["last_updated"])
        for lot in d["lots"]:
            c = lot.get("coords")
            if not c or not in_bbox(c["lat"], c["lng"]) or lot.get("state") in ("nodata", "closed"):
                continue
            readings = [(ts, "parking_free", float(lot["free"]), "spaces")]
            if lot.get("total"):
                readings.append((ts, "parking_occupancy", 100 * (1 - lot["free"] / lot["total"]), "%"))
            yield ({"key": f"park:{lot['id']}", "name": f"{lot['name']} ({city})", "lat": c["lat"], "lon": c["lng"],
                    "meta": {"city": city, "type": lot.get("lot_type"), "total": lot.get("total")}}, readings)


SOURCES = {"sensor_community": sensor_community, "opensensemap": opensensemap, "dwd": dwd,
           "pegelonline": pegelonline, "bfs_odl": bfs_odl, "bfs_uv": bfs_uv, "uba": uba,
           "dwd_pollen": dwd_pollen, "parking": parking}


class Ids:
    """In-memory cache of quantity ids, so inserts need no lookups."""

    def __init__(self, db):
        self.q = dict(db.execute("SELECT name, id FROM quantities"))

    def quantity(self, db, name, unit):
        if name not in self.q:
            self.q[name] = db.execute("INSERT INTO quantities (name, unit) VALUES (?,?)", (name, unit)).lastrowid
        return self.q[name]


def store(db, ids, source, items, now):
    """Write one source's results; returns (stations, new readings)."""
    db.execute("CREATE TEMP TABLE IF NOT EXISTS batch (sid, qid, ts, value, PRIMARY KEY (sid, qid, ts)) WITHOUT ROWID")
    db.execute("DELETE FROM batch")
    seen = set()
    for st, readings in items:  # a station may appear more than once (e.g. sensor.community)
        # No RETURNING: Debian bullseye ships SQLite 3.34
        db.execute("""INSERT INTO stations (key, source, name, lat, lon, meta, last_seen) VALUES (?,?,?,?,?,?,?)
                      ON CONFLICT(key) DO UPDATE SET name=excluded.name, lat=excluded.lat, lon=excluded.lon,
                        meta=excluded.meta, last_seen=excluded.last_seen""",
                   (st["key"], source, st["name"], st["lat"], st["lon"],
                    json.dumps(st["meta"], ensure_ascii=False), now))
        sid = db.execute("SELECT id FROM stations WHERE key = ?", (st["key"],)).fetchone()[0]
        seen.add(sid)
        db.executemany("INSERT OR IGNORE INTO batch VALUES (?,?,?,?)",
                       [(sid, ids.quantity(db, ALIASES.get(q, q), UNITS.get(ALIASES.get(q, q), unit)), ts, v)
                        for ts, q, v, unit in readings if q])
    # Keep only readings we don't have yet, then fan them out to raw, hourly and latest
    db.execute("""DELETE FROM batch WHERE ts < ? OR EXISTS (SELECT 1 FROM readings r
                  WHERE r.sid = batch.sid AND r.qid = batch.qid AND r.ts = batch.ts)""", (now - RAW_DAYS * 86400,))
    n = db.execute("INSERT INTO readings SELECT sid, qid, ts, value FROM batch").rowcount
    db.execute("""INSERT INTO hourly SELECT sid, qid, ts / 3600 * 3600, SUM(value), COUNT(*), MIN(value), MAX(value)
                  FROM batch WHERE true GROUP BY 1, 2, 3
                  ON CONFLICT (sid, qid, hour) DO UPDATE SET sum = sum + excluded.sum, n = n + excluded.n,
                    min = MIN(min, excluded.min), max = MAX(max, excluded.max)""")
    db.execute("""INSERT INTO latest SELECT qid, sid, MAX(ts), value FROM batch WHERE true GROUP BY qid, sid
                  ON CONFLICT (qid, sid) DO UPDATE SET ts = excluded.ts, value = excluded.value
                  WHERE excluded.ts > latest.ts""")
    return len(seen), n


def prune(db, now):
    """Drop old raw readings and hourly rows, at most once a day. Deletes go series by series via the primary key."""
    last = db.execute("SELECT value FROM kv WHERE key = 'pruned'").fetchone()
    if last and now - int(last[0]) < 86400:
        return
    series = db.execute("SELECT sid, qid FROM latest").fetchall()
    db.executemany("DELETE FROM readings WHERE sid = ? AND qid = ? AND ts < ?",
                   [(s, q, now - RAW_DAYS * 86400) for s, q in series])
    db.executemany("DELETE FROM hourly WHERE sid = ? AND qid = ? AND hour < ?",
                   [(s, q, now - HOURLY_DAYS * 86400) for s, q in series])
    db.execute("INSERT OR REPLACE INTO kv VALUES ('pruned', ?)", (str(now),))
    db.commit()


def collect(db, log=print):
    now = int(time.time())
    ids = Ids(db)

    def run(fn):
        t0 = time.time()
        return list(fn()), time.time() - t0

    with ThreadPoolExecutor(len(SOURCES)) as pool:  # network-bound; DB writes stay on this thread
        futures = {pool.submit(run, fn): name for name, fn in SOURCES.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            n_st = n_rd = 0
            err = secs = None
            try:
                items, secs = fut.result()
                n_st, n_rd = store(db, ids, name, items, now)
            except Exception as e:  # keep other sources running
                db.rollback()
                ids = Ids(db)  # quantities inserted in the rolled-back transaction are gone again
                err = f"{type(e).__name__}: {e}"
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?)", (now, name, n_st, n_rd, err, secs))
            db.commit()
            log(f"{name:17} {n_st:5} stations {n_rd:6} new readings" + (f"  ERROR {err}" if err else ""))
    prune(db, now)


def summary(db):
    print("Stations seen in the last 24 h, by source:")
    for row in db.execute("""SELECT source, COUNT(*) FROM stations
                             WHERE last_seen > CAST(strftime('%s','now') AS INTEGER) - 86400
                             GROUP BY source ORDER BY 2 DESC"""):
        print(f"  {row[0]:17} {row[1]}")
    print("Series:", db.execute("SELECT COUNT(*) FROM latest").fetchone()[0],
          " raw readings:", db.execute("SELECT COUNT(*) FROM readings").fetchone()[0],
          " hourly rows:", db.execute("SELECT COUNT(*) FROM hourly").fetchone()[0])
    print("Top quantities:")
    for q, n in db.execute("""SELECT q.name, COUNT(*) FROM latest l JOIN quantities q ON q.id = l.qid
                              GROUP BY q.name ORDER BY 2 DESC LIMIT 20"""):
        print(f"  {q:28} {n} stations")


def main():
    db = connect()
    summary(db) if sys.argv[1:] == ["summary"] else collect(db)


if __name__ == "__main__":
    main()
