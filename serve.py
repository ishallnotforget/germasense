#!/usr/bin/env python3
"""Live map of the collected sensors.

Usage:
    python3 serve.py [--port 8642] [--no-collect]

Serves static/index.html plus a small JSON API on the SQLite database written by collector.py.
While running it also calls the collector every 10 minutes (disable with --no-collect).
"""
import argparse
import json
import sqlite3
import threading
import time
import urllib.parse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import buses
import collector

BUSES = buses.Buses()  # fetches the realtime feed lazily, only while someone has the bus layer open
STATIC = Path(__file__).with_name("static")
COLLECT_EVERY_S = 600
MAX_AGE = "-6 hours"  # readings older than this are not shown as "current" on the map


def db():
    con = sqlite3.connect(collector.DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con


# Physically plausible ranges; cheap sensors report e.g. -40 °C or 999.9 µg/m³ when broken
PLAUSIBLE = {"temperature": (-30, 50), "air_temperature": (-30, 50), "dew_point": (-40, 35),
             "humidity": (0.5, 100), "relative_humidity": (0.5, 100), "pressure": (850, 1090),
             "pressure_msl": (900, 1090), "pm10": (0, 999), "pm2_5": (0, 999), "pm1": (0, 999), "co2": (250, 10000)}


def latest_for_quantity(con, quantity):
    lo, hi = PLAUSIBLE.get(quantity, (-1e12, 1e12))
    return [dict(r) for r in con.execute("""
        SELECT s.id, s.name, s.source, s.lat, s.lon, r.value, r.unit, r.ts
        FROM readings r JOIN stations s ON s.id = r.station_id
        WHERE r.quantity = ? AND r.ts > datetime('now', ?) AND s.lat IS NOT NULL AND r.value BETWEEN ? AND ?
          AND r.ts = (SELECT MAX(ts) FROM readings x WHERE x.station_id = r.station_id AND x.quantity = r.quantity)
        """, (quantity, MAX_AGE, lo, hi))]


def quantities(con):
    return [dict(r) for r in con.execute("""
        SELECT quantity, MAX(unit) AS unit, COUNT(DISTINCT station_id) AS stations
        FROM readings r JOIN stations s ON s.id = r.station_id
        WHERE r.ts > datetime('now', ?) AND s.lat IS NOT NULL
        GROUP BY quantity ORDER BY stations DESC, quantity""", (MAX_AGE,))]


def station(con, sid):
    st = con.execute("SELECT * FROM stations WHERE id = ?", (sid,)).fetchone()
    if not st:
        return None
    latest = [dict(r) for r in con.execute("""
        SELECT quantity, value, unit, MAX(ts) AS ts FROM readings
        WHERE station_id = ? GROUP BY quantity ORDER BY quantity""", (sid,))]
    return {**dict(st), "meta": json.loads(st["meta"] or "{}"), "latest": latest}


def series(con, sid, quantity, hours):
    return [[r[0], r[1]] for r in con.execute("""
        SELECT ts, value FROM readings WHERE station_id = ? AND quantity = ? AND ts > datetime('now', ?)
        ORDER BY ts""", (sid, quantity, f"-{int(hours)} hours"))]


def status(con):
    rows = con.execute("""SELECT source, started, stations, readings, error FROM runs
                          WHERE started = (SELECT MAX(started) FROM runs)""").fetchall()
    return {"last_run": rows[0]["started"] if rows else None, "sources": [dict(r) for r in rows],
            "pollen": [dict(r) for r in con.execute("""
                SELECT quantity, value, ts FROM readings WHERE station_id = 'pollen:31'
                AND ts = (SELECT MAX(ts) FROM readings WHERE station_id = 'pollen:31') ORDER BY quantity""")]}


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
        con = db()
        try:
            route = url.path[5:]
            if route == "quantities":
                body = quantities(con)
            elif route == "latest":
                body = latest_for_quantity(con, q.get("quantity", "pm10"))
            elif route == "station":
                body = station(con, q.get("id", ""))
            elif route == "series":
                body = series(con, q.get("id", ""), q.get("quantity", ""), q.get("hours", "48"))
            elif route == "status":
                body = status(con)
            elif route == "buses":
                bbox = tuple(map(float, q["bbox"].split(","))) if q.get("bbox") else None  # lat0,lon0,lat1,lon1
                body = BUSES.vehicles(bbox=bbox)
            else:
                body = None
        finally:
            con.close()
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(200 if body is not None else 404)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)


def collect_loop():
    while True:
        try:
            con = sqlite3.connect(collector.DB_PATH, timeout=30)
            con.executescript(collector.SCHEMA)
            collector.collect(con)
            con.close()
        except Exception as e:
            print("collect failed:", e)
        time.sleep(COLLECT_EVERY_S)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8642)
    ap.add_argument("--no-collect", action="store_true")
    args = ap.parse_args()
    con = sqlite3.connect(collector.DB_PATH)
    con.executescript(collector.SCHEMA)
    con.close()
    if not args.no_collect:
        threading.Thread(target=collect_loop, daemon=True).start()
    print(f"Serving on http://localhost:{args.port}")
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
