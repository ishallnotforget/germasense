#!/usr/bin/env python3
"""Live map of the collected sensors and of buses and trains.

Usage:
    python3 serve.py [--port 8642] [--host 127.0.0.1] [--no-collect]

Serves static/index.html plus a small JSON API on the SQLite database written by collector.py,
and the live overlays from layers.py (aircraft, Autobahn, warnings, earthquakes).
While running it also calls the collector every 10 minutes (disable with --no-collect).
Map responses are cached in memory until the next collection and sent gzipped.
"""
import argparse
import gzip
import json
import sqlite3
import threading
import time
import urllib.parse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import buses
import collector
import layers

BUSES = buses.Buses()  # fetches the realtime feed lazily, only while someone has the bus layer open
LAYERS = layers.Layers()  # aircraft, Autobahn, warnings, earthquakes: also fetched only while shown
STATIC = Path(__file__).with_name("static")
COLLECT_EVERY_S = 600
MAX_AGE_S = 6 * 3600  # readings older than this are not shown as "current" on the map

# Physically plausible ranges; cheap sensors report e.g. -40 °C or 999.9 µg/m³ when broken
PLAUSIBLE = {"temperature": (-30, 50), "dew_point": (-40, 35), "soil_temperature": (-20, 45),
             "humidity": (0.5, 100), "soil_moisture": (0, 100), "pressure": (700, 1090), "pressure_msl": (900, 1090),
             "pm10": (0, 999), "pm2_5": (0, 999), "pm1": (0, 999), "pm4": (0, 999), "co2": (250, 10000),
             "wind_speed": (0, 200), "wind_gust": (0, 250), "wind_direction": (0, 360), "noise": (20, 140),
             "rain_rate": (0, 300), "illuminance": (0, 200000)}


def db():
    con = sqlite3.connect(f"file:{collector.DB_PATH}?mode=ro", uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    return con


class Cache:
    """Responses that only change when a collection finishes: cleared by bump(), and after TTL_S anyway."""
    TTL_S = 120

    def __init__(self):
        self.lock, self.data = threading.Lock(), {}

    def get(self, key, make):
        with self.lock:
            hit = self.data.get(key)
        if hit is None or time.time() - hit[0] > self.TTL_S:
            hit = (time.time(), make())
            with self.lock:
                self.data[key] = hit
        return hit[1]

    def bump(self):
        with self.lock:
            self.data.clear()


CACHE = Cache()


def latest_for_quantity(con, quantity):
    """Compact rows [id, lat, lon, value, ts, name, source] so ~5000 points stay small."""
    lo, hi = PLAUSIBLE.get(quantity, (-1e12, 1e12))
    q = con.execute("SELECT id, unit FROM quantities WHERE name = ?", (quantity,)).fetchone()
    if not q:
        return {"unit": None, "rows": []}
    rows = con.execute("""
        SELECT s.id, s.lat, s.lon, l.value, l.ts, s.name, s.source
        FROM latest l JOIN stations s ON s.id = l.sid
        WHERE l.qid = ? AND l.ts > ? AND s.lat IS NOT NULL AND l.value BETWEEN ? AND ?""",
                       (q["id"], int(time.time()) - MAX_AGE_S, lo, hi)).fetchall()
    return {"unit": q["unit"], "rows": [[r[0], round(r[1], 5), round(r[2], 5), round(r[3], 3), r[4], r[5], r[6]]
                                        for r in rows]}


def quantities(con):
    return [dict(r) for r in con.execute("""
        SELECT q.name AS quantity, q.unit, COUNT(*) AS stations
        FROM latest l JOIN quantities q ON q.id = l.qid JOIN stations s ON s.id = l.sid
        WHERE l.ts > ? AND s.lat IS NOT NULL
        GROUP BY q.id ORDER BY stations DESC, q.name""", (int(time.time()) - MAX_AGE_S,))]


def station(con, sid):
    st = con.execute("SELECT id, source, name, lat, lon, meta FROM stations WHERE id = ?", (sid,)).fetchone()
    if not st:
        return None
    latest = [dict(r) for r in con.execute("""
        SELECT q.name AS quantity, l.value, q.unit, l.ts FROM latest l JOIN quantities q ON q.id = l.qid
        WHERE l.sid = ? ORDER BY q.name""", (sid,))]
    return {**dict(st), "meta": json.loads(st["meta"] or "{}"), "latest": latest}


def series(con, sid, quantity, hours):
    """Raw readings up to 48 h, hourly averages beyond. Returns [[unix ts, value], ...]."""
    since = int(time.time()) - int(hours) * 3600
    q = con.execute("SELECT id FROM quantities WHERE name = ?", (quantity,)).fetchone()
    if not q:
        return []
    if int(hours) <= 48:
        sql = "SELECT ts, value FROM readings WHERE sid = ? AND qid = ? AND ts > ? ORDER BY ts"
    else:
        sql = "SELECT hour + 1800, sum / n FROM hourly WHERE sid = ? AND qid = ? AND hour > ? ORDER BY hour"
    return [[r[0], r[1]] for r in con.execute(sql, (sid, q["id"], since))]


def status(con):
    rows = con.execute("""SELECT source, started, stations, readings, error, seconds FROM runs
                          WHERE started = (SELECT MAX(started) FROM runs)""").fetchall()
    pollen = {}
    for r in con.execute("""
            SELECT s.key, s.name, q.name AS quantity, l.value FROM stations s
            JOIN latest l ON l.sid = s.id JOIN quantities q ON q.id = l.qid
            WHERE s.source = 'dwd_pollen' ORDER BY s.key, q.name"""):
        pollen.setdefault(r["key"], {"region": r["name"], "values": {}})["values"][r["quantity"]] = r["value"]
    return {"last_run": rows[0]["started"] if rows else None, "sources": [dict(r) for r in rows],
            "pollen": pollen}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(STATIC), **kw)

    def log_message(self, *a):
        pass

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if not url.path.startswith("/api/"):
            return super().do_GET()
        q = {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}
        route = url.path[5:]
        try:
            data = self.api(route, q)
        except (ValueError, KeyError):
            data = None
        raw, pre = data if isinstance(data, tuple) else (data, None)
        code = 200 if raw is not None else 404
        raw = raw if raw is not None else b"null"
        gz = "gzip" in self.headers.get("Accept-Encoding", "") and len(raw) > 1400
        body = (pre or gzip.compress(raw, 5)) if gz else raw
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        if gz:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def api(self, route, q):
        """Returns JSON bytes, a cached (plain, gzipped) pair, or None for 404."""
        def cached(key, make):
            def build():
                con = db()
                try:
                    raw = json.dumps(make(con), ensure_ascii=False, separators=(",", ":")).encode()
                finally:
                    con.close()
                return raw, gzip.compress(raw, 6)
            return CACHE.get(key, build)

        if route == "quantities":
            return cached("q", quantities)
        if route == "latest":
            quantity = q.get("quantity", "pm10")
            return cached(("l", quantity), lambda con: latest_for_quantity(con, quantity))
        if route == "status":
            return cached("s", status)
        if route.startswith("layer/"):
            return LAYERS.get(route[6:])
        if route == "buses":
            bbox = tuple(map(float, q["bbox"].split(","))) if q.get("bbox") else None  # lat0,lon0,lat1,lon1
            return json.dumps(BUSES.vehicles(bbox=bbox, rail_only=q.get("rail") == "1"), ensure_ascii=False, separators=(",", ":")).encode()
        con = db()
        try:
            if route == "station":
                body = station(con, int(q["id"]))
            elif route == "series":
                body = series(con, int(q["id"]), q.get("quantity", ""), q.get("hours", "48"))
            else:
                return None
        finally:
            con.close()
        return None if body is None else json.dumps(body, ensure_ascii=False).encode()


def collect_loop():
    while True:
        t0 = time.time()
        try:
            con = collector.connect()
            collector.collect(con, log=lambda *a: None)
            con.close()
        except Exception as e:
            print("collect failed:", e)
        CACHE.bump()
        time.sleep(max(60, COLLECT_EVERY_S - (time.time() - t0)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8642)
    ap.add_argument("--host", default="127.0.0.1", help="use 0.0.0.0 to serve your whole network")
    ap.add_argument("--no-collect", action="store_true")
    args = ap.parse_args()
    collector.connect().close()  # create the schema before the first read-only request
    if not args.no_collect:
        threading.Thread(target=collect_loop, daemon=True).start()
    print(f"Serving on http://{'localhost' if args.host in ('127.0.0.1', '0.0.0.0') else args.host}:{args.port}")
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
