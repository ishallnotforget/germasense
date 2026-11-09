# Osnabrück sensor & transit map

A live map of about 850 public sensors and every bus, tram and train between Quakenbrück and Essen.
It runs on your own computer and needs only Python, with no extra packages and no API keys.

## Installation

You need **Python 3.9 or newer**. Nothing else: no `pip install`, no API keys.

**1. Install Python** if `python3 --version` (Windows: `py --version`) doesn't show 3.9 or newer.

- **Windows:**
  - Install from <https://www.python.org/downloads/>, or run `winget install Python.Python.3.12`.
  - Then use `py` wherever this README says `python3`.
- **macOS:**
  - Install from <https://www.python.org/downloads/>, or run `brew install python`.
- **Linux:**
  - Python is usually already installed.
  - Otherwise, on Debian or Ubuntu: `sudo apt install python3`.

**2. Get the code**, either by cloning it:

```
git clone https://github.com/ishallnotforget/osna-sensors.git
cd osna-sensors
```

or by downloading it: use **Code → Download ZIP** on GitHub, unpack it and open a terminal in the folder.

**3. Run it**

```
python3 serve.py
```

Then open <http://localhost:8642> in your browser. Stop it with Ctrl+C.

Optional flags:
- `--port 9000`: use another port.
- `--no-collect`: serve existing data only, without collecting new data in the background.

The server only listens on your own computer, so other devices on your network can't reach it.

To update to a newer version later, run `git pull` in the folder.

**First start:**
- **Sensors:** the first collection runs right away and takes about 20 seconds. After that, the server collects again every 10 minutes for as long as it is running. Reload the page after a minute.
- **Buses & trains:** the first time you tick "Buses & trains", the server downloads the national timetable. It reads about 270 MB of the 276 MB file and builds a local `gtfs.db` of about 180 MB, which takes around 2 minutes. It repeats this once a week. Until it finishes, the vehicle layer stays empty.

The data lives in `osna.db` and `gtfs.db` next to the scripts. You can delete them at any time; they are rebuilt.

## What's on the map

- **Sensor dropdown:** pick a quantity, such as PM10, temperature, water level, radiation or NO₂. Click a station to see a chart for the last 24 h, 48 h or 7 days.
- **Buses & trains:** colour shows punctuality. Dashed outlines mean timetable only, with no live data. Type line names into the filter field, e.g. `M3`, `RE18` or `107 U11`.
- **Pollen:** chips for western Lower Saxony appear in the header.

## Data sources

- **Sensors:**
  - Sensor.Community
  - openSenseMap
  - DWD weather stations (via Bright Sky)
  - WSV Pegelonline (water levels)
  - BfS gamma dose rate and UV index
  - Umweltbundesamt air quality
  - DWD pollen forecast
- **Transit:** the timetable and realtime feeds from gtfs.de.
- **Map tiles:** © OpenStreetMap contributors.

Please keep the 10-minute collection interval; these are free public services.

## Files

| File | Purpose |
|---|---|
| `serve.py` | Web server and JSON API, runs the collector in the background |
| `collector.py` | Fetches all sensor sources into `osna.db`. Run it on its own: `python3 collector.py`, or `python3 collector.py summary` |
| `buses.py` | Timetable cache and realtime vehicle positions. Rebuild the cache with `python3 buses.py refresh` |
| `static/index.html` | The map |

Vehicle positions are estimated from timetable and realtime delays, not GPS.
