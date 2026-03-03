#!/usr/bin/env python3
"""
Interactive test tool — write mock alerts to Firebase RTDB
and inject mock Telegram messages into intel.db.

Usage:
    python test_alerts.py
"""

import json
import math
import re
import subprocess
import time
import uuid
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, db

FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app/"
FIREBASE_NODE = "/public_state/active_alerts"
SERVICE_ACCOUNT = Path(__file__).parent / "serviceAccountKey.json"
POLYGONS_FILE = Path(__file__).parent / "polygons.json"

STATUSES = ["alert", "pre_alert", "after_alert", "telegram_yellow", "uav", "terrorist"]

INTEL_CHANNELS = ["beforeredalert", "yemennews7071"]


def _sanitize_fb_key(key: str) -> str:
    return re.sub(r'[.$/\[\]#]', '_', key)


def _make_payload(city_he: str, city_name: str, status: str, is_double: bool = False) -> dict:
    return {
        "id": f"alert_{city_he}",
        "city_name": city_name,
        "city_name_he": city_he,
        "timestamp": int(time.time() * 1000),
        "is_double": is_double,
        "status": status,
    }


def cmd_add_alert(polygons: dict):
    query = input("Search city (Hebrew): ").strip()
    if not query:
        return
    matches = [k for k in polygons if query in k][:20]
    if not matches:
        print("No matches found.")
        return
    for i, m in enumerate(matches):
        print(f"  {i}: {m}")
    try:
        idx = int(input("Pick index: ").strip())
        city_he = matches[idx]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    print("Status:")
    for i, s in enumerate(STATUSES):
        print(f"  {i}: {s}")
    try:
        status = STATUSES[int(input("Pick status: ").strip())]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    is_double = input("Double/salvo? (y/N): ").strip().lower() == "y"

    payload = _make_payload(city_he, polygons[city_he]["city_name"], status, is_double)
    ref = db.reference(f"{FIREBASE_NODE}/{_sanitize_fb_key(city_he)}")
    ref.set(payload)
    print(f"Written: {city_he} = {status}")


def cmd_batch_alert(polygons: dict):
    query = input("Search cities containing (Hebrew): ").strip()
    if not query:
        return
    matches = [k for k in polygons if query in k]
    print(f"Found {len(matches)} cities matching '{query}'")
    if not matches:
        return

    limit = input(f"How many to alert? (max {len(matches)}, Enter=all): ").strip()
    limit = int(limit) if limit else len(matches)
    matches = matches[:limit]

    print("Status:")
    for i, s in enumerate(STATUSES):
        print(f"  {i}: {s}")
    try:
        status = STATUSES[int(input("Pick status: ").strip())]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    batch = {}
    for city_he in matches:
        key = _sanitize_fb_key(city_he)
        batch[key] = _make_payload(city_he, polygons[city_he]["city_name"], status)

    ref = db.reference(FIREBASE_NODE)
    ref.update(batch)
    print(f"Written {len(batch)} alerts with status={status}")


def cmd_clear_all():
    db.reference(FIREBASE_NODE).set({})
    print("All alerts cleared.")


def cmd_clear_one(polygons: dict):
    query = input("Search city to clear (Hebrew): ").strip()
    if not query:
        return
    matches = [k for k in polygons if query in k][:20]
    if not matches:
        print("No matches found.")
        return
    for i, m in enumerate(matches):
        print(f"  {i}: {m}")
    try:
        idx = int(input("Pick index: ").strip())
        city_he = matches[idx]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    ref = db.reference(f"{FIREBASE_NODE}/{_sanitize_fb_key(city_he)}")
    ref.delete()
    print(f"Cleared: {city_he}")


def cmd_show_active():
    data = db.reference(FIREBASE_NODE).get()
    if not data:
        print("No active alerts.")
        return
    print(f"\n{'City':<25} {'Status':<18} {'Double'}")
    print("-" * 55)
    for key, val in data.items():
        print(f"{val.get('city_name_he', key):<25} {val.get('status', '?'):<18} {val.get('is_double', False)}")
    print(f"\nTotal: {len(data)}")


DOCKER_CONTAINER = "telegram-intel"
DOCKER_DB_PATH = "/usr/src/app/data/intel.db"


def _docker_sqlite(sql: str) -> str:
    """Run a SQL statement inside the Docker container's intel.db."""
    cmd = ["docker", "exec", DOCKER_CONTAINER, "sqlite3", DOCKER_DB_PATH, sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        raise RuntimeError(f"docker exec failed: {result.stderr.strip()}")
    return result.stdout.strip()


TELEGRAM_PRESETS = [
    ("שיגורים לעבר צפון", "Alert keyword (שיגורים) + region (צפון)"),
    ("שיגורים לעבר דרום", "Alert keyword (שיגורים) + region (דרום)"),
    ("שיגורים לעבר מרכז", "Alert keyword (שיגורים) + region (מרכז)"),
    ("יציאות מעזה לעבר נגב", "Alert keyword (יציאות) + region (נגב)"),
    ("ניתן לצאת מהמרחב המוגן", "Cancel keyword — should NOT trigger yellow"),
    ("Custom message", "Write your own message"),
]


def cmd_telegram_inject():
    """Inject a mock Telegram message into the Docker container's intel.db."""
    print("\nPick a message to inject:")
    for i, (msg, desc) in enumerate(TELEGRAM_PRESETS):
        print(f"  {i}: {desc}")
    try:
        idx = int(input("Pick preset: ").strip())
        msg, _ = TELEGRAM_PRESETS[idx]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    if msg == "Custom message":
        msg = input("Enter message text (Hebrew): ").strip()
        if not msg:
            return

    print("Channel:")
    for i, ch in enumerate(INTEL_CHANNELS):
        print(f"  {i}: {ch}")
    try:
        channel = INTEL_CHANNELS[int(input("Pick channel: ").strip())]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    event_id = str(uuid.uuid4())
    now = time.time()

    # Escape single quotes for SQL
    safe_msg = msg.replace("'", "''")
    sql = (
        f"INSERT INTO event_sources (event_id, raw_text, channel, reported_at) "
        f"VALUES ('{event_id}', '{safe_msg}', '{channel}', {now});"
    )

    try:
        _docker_sqlite(sql)
        print(f"Injected into container's intel.db: [{channel}] \"{msg}\"")
        print("brain.py will pick this up on next poll cycle.")
    except RuntimeError as e:
        print(f"Failed to inject: {e}")
        print("Make sure the telegram-intel container is running.")


def cmd_telegram_show():
    """Show recent messages in the Docker container's intel DB."""
    sql = (
        "SELECT channel, reported_at, SUBSTR(raw_text, 1, 50) as msg "
        "FROM event_sources ORDER BY reported_at DESC LIMIT 20;"
    )

    try:
        output = _docker_sqlite(f"-header -separator '|' \"{sql}\"")
    except RuntimeError as e:
        print(f"Failed to query: {e}")
        print("Make sure the telegram-intel container is running.")
        return

    if not output:
        print("No messages in intel DB.")
        return

    print(f"\n{'Channel':<20} {'Age':>6}  Message")
    print("-" * 70)
    now = time.time()
    for line in output.split("\n"):
        if line.startswith("channel"):  # skip header
            continue
        parts = line.split("|", 2)
        if len(parts) < 3:
            continue
        channel, ts_str, msg = parts
        try:
            age = int(now - float(ts_str))
        except ValueError:
            age = 0
        print(f"{channel:<20} {age:>4}s  {msg}")


FIREBASE_UAV_NODE = "/public_state/uav_tracks"

# Cities along a north-south corridor (western Galilee → coast)
# Simulates a UAV entering from Lebanon heading south
UAV_FLIGHT_PRESETS = {
    # Each value is a list of "waves". Each wave is a list of cities that alert together.
    # Single-city waves = one alert at a time (old behavior).
    # Multi-city waves = multiple alerts at once (realistic).
    "Lebanon → Western Galilee (single alerts)": [
        ["ראש הנקרה"], ["שלומי"], ["מצובה"], ["גשר הזיו"], ["נהריה"], ["עכו"],
    ],
    "Lebanon → Central Galilee (single alerts)": [
        ["זרעית"], ["שומרה"], ["אבן מנחם"], ["גורנות הגליל"], ["מעלות תרשיחא"],
    ],
    "Lebanon → Western Galilee (batch alerts)": [
        ["ראש הנקרה", "שלומי", "מצובה"],
        ["גשר הזיו", "נהריה"],
        ["עכו"],
    ],
    "Lebanon → Central Galilee (batch alerts)": [
        ["זרעית", "שומרה"],
        ["אבן מנחם", "גורנות הגליל"],
        ["מעלות תרשיחא"],
    ],
}


def _centroid(polygon_coords: list) -> tuple[float, float]:
    """Average of boundary points → (lat, lng)."""
    n = len(polygon_coords)
    if n == 0:
        return (0.0, 0.0)
    return (sum(p[0] for p in polygon_coords) / n, sum(p[1] for p in polygon_coords) / n)


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlon = math.radians(lon2 - lon1)
    la1, la2 = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(la2)
    y = math.cos(la1) * math.sin(la2) - math.sin(la1) * math.cos(la2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _project_point(lat: float, lon: float, bearing_deg: float, dist_km: float) -> tuple[float, float]:
    R = 6371.0
    d = dist_km / R
    brng = math.radians(bearing_deg)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    lat2 = math.asin(math.sin(lat1) * math.cos(d) + math.cos(lat1) * math.sin(d) * math.cos(brng))
    lon2 = lon1 + math.atan2(math.sin(brng) * math.sin(d) * math.cos(lat1),
                              math.cos(d) - math.sin(lat1) * math.sin(lat2))
    return (math.degrees(lat2), math.degrees(lon2))


def _project_onto_line(point_lat, point_lng, origin_lat, origin_lng, bearing_deg):
    """Project a point onto a line defined by origin + bearing."""
    dist = _haversine_km(origin_lat, origin_lng, point_lat, point_lng)
    if dist < 0.01:
        return (origin_lat, origin_lng, 0.0)
    brng_to_point = _bearing(origin_lat, origin_lng, point_lat, point_lng)
    angle_diff = math.radians(brng_to_point - bearing_deg)
    along = dist * math.cos(angle_diff)
    plat, plng = _project_point(origin_lat, origin_lng, bearing_deg, along)
    return (plat, plng, along)


UAV_MIN_SPEED_KMH = 80
UAV_MAX_SPEED_KMH = 300
UAV_DEFAULT_SPEED_KMH = 180
SPEED_SMOOTHING = 0.3


class _TestUavTrack:
    """Mirrors brain.py UavTrack for test simulation."""
    def __init__(self, track_id, lat, lng, ts):
        self.track_id = track_id
        self.raw_centroids = [(lat, lng, ts)]
        self.smoothed_points = [(lat, lng, ts)]
        self.heading = 0.0
        self.speed_estimate = UAV_DEFAULT_SPEED_KMH

    def add_point(self, raw_lat, raw_lng, now):
        self.raw_centroids.append((raw_lat, raw_lng, now))
        first = self.raw_centroids[0]

        if len(self.raw_centroids) == 2:
            self.heading = _bearing(first[0], first[1], raw_lat, raw_lng)
            _, _, proj_dist = _project_onto_line(raw_lat, raw_lng, first[0], first[1], self.heading)
            proj_dist = max(proj_dist, 0.5)
            new_lat, new_lng = _project_point(first[0], first[1], self.heading, proj_dist)
            self.smoothed_points.append((new_lat, new_lng, now))
            dt = now - first[2]
            if dt > 0:
                self.speed_estimate = max(UAV_MIN_SPEED_KMH, min(UAV_MAX_SPEED_KMH, (proj_dist / dt) * 3600))
        else:
            self.heading = _bearing(first[0], first[1], raw_lat, raw_lng)
            last_sp = self.smoothed_points[-1]
            dt = now - last_sp[2]
            advance_km = max((self.speed_estimate / 3600) * dt, 0.5)
            new_lat, new_lng = _project_point(last_sp[0], last_sp[1], self.heading, advance_km)
            self.smoothed_points.append((new_lat, new_lng, now))
            _, _, raw_proj_dist = _project_onto_line(raw_lat, raw_lng, last_sp[0], last_sp[1], self.heading)
            raw_proj_dist = max(raw_proj_dist, 0.0)
            if dt > 0:
                raw_speed = max(UAV_MIN_SPEED_KMH, min(UAV_MAX_SPEED_KMH, (raw_proj_dist / dt) * 3600))
                self.speed_estimate = SPEED_SMOOTHING * raw_speed + (1 - SPEED_SMOOTHING) * self.speed_estimate

    def to_firebase(self):
        observed = [[p[0], p[1]] for p in self.smoothed_points]
        predicted = []
        if len(self.smoothed_points) >= 2:
            last = self.smoothed_points[-1]
            for secs in [30, 60]:
                pred_dist = (self.speed_estimate / 3600) * secs
                if pred_dist > 0:
                    plat, plng = _project_point(last[0], last[1], self.heading, pred_dist)
                    predicted.append([plat, plng])
        return {
            "track_id": self.track_id,
            "observed": observed,
            "predicted": predicted,
            "heading_deg": round(self.heading, 1),
            "speed_kmh": round(self.speed_estimate, 0),
            "last_updated": int(self.smoothed_points[-1][2] * 1000),
        }


def cmd_simulate_uav(polygons: dict):
    """Simulate a UAV flight path — writes directly to uav_tracks (no brain.py needed)."""
    print("\nUAV flight path presets:")
    preset_names = list(UAV_FLIGHT_PRESETS.keys())
    for i, name in enumerate(preset_names):
        waves = UAV_FLIGHT_PRESETS[name]
        total_cities = sum(len(w) for w in waves)
        print(f"  {i}: {name} ({total_cities} cities, {len(waves)} waves)")

    try:
        idx = int(input("Pick preset: ").strip())
        preset_name = preset_names[idx]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    waves = UAV_FLIGHT_PRESETS[preset_name]
    # Filter each wave to valid cities
    valid_waves = []
    for wave in waves:
        valid_cities = [c for c in wave if c in polygons and polygons[c].get("polygon")]
        if valid_cities:
            valid_waves.append(valid_cities)

    if not valid_waves:
        print("No valid cities in preset!")
        return

    total_cities = sum(len(w) for w in valid_waves)

    delay = input("Delay between waves in seconds (default 3): ").strip()
    delay = float(delay) if delay else 3.0

    print(f"\nSimulating UAV flight: {total_cities} cities in {len(valid_waves)} waves, {delay}s apart")
    for i, wave in enumerate(valid_waves):
        print(f"  Wave {i+1}: {', '.join(wave)}")
    print("Starting in 2 seconds...\n")
    time.sleep(2)

    alert_ref = db.reference(FIREBASE_NODE)
    uav_ref = db.reference(FIREBASE_UAV_NODE)
    track_id = "uav_test_0"
    track: _TestUavTrack | None = None
    city_count = 0

    for wi, wave in enumerate(valid_waves):
        now = time.time()

        # Compute average centroid of all cities in this wave
        lats, lngs = [], []
        for city_he in wave:
            lat, lng = _centroid(polygons[city_he]["polygon"])
            lats.append(lat)
            lngs.append(lng)

            # Write the alert polygon for each city
            payload = _make_payload(city_he, polygons[city_he]["city_name"], "uav")
            key = _sanitize_fb_key(city_he)
            alert_ref.child(key).set(payload)

        # Use average centroid of the wave as the single observation point
        avg_lat = sum(lats) / len(lats)
        avg_lng = sum(lngs) / len(lngs)

        if track is None:
            track = _TestUavTrack(track_id, avg_lat, avg_lng, now)
        else:
            track.add_point(avg_lat, avg_lng, now)

        # Push track data
        track_payload = track.to_firebase()
        uav_ref.set({track_id: track_payload})

        city_count += len(wave)
        cities_str = ", ".join(wave)
        print(f"  Wave {wi+1}/{len(valid_waves)}: [{cities_str}]  →  track updated ({len(track.smoothed_points)} pts)")

        if wi < len(valid_waves) - 1:
            time.sleep(delay)

    print(f"\nDone! {city_count} UAV alerts in {len(valid_waves)} waves written to Firebase.")
    print("Check the map — drone should be visible now.")


def cmd_clear_uav_tracks():
    """Clear all UAV flight path tracks from Firebase."""
    db.reference(FIREBASE_UAV_NODE).set({})
    print("UAV tracks cleared.")


def main():
    # Load polygons
    with open(POLYGONS_FILE, encoding="utf-8") as f:
        polygons = json.load(f)

    # Init Firebase
    cred = credentials.Certificate(str(SERVICE_ACCOUNT))
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})

    print(f"Loaded {len(polygons)} cities.")
    print(f"Telegram inject target: docker exec {DOCKER_CONTAINER} → {DOCKER_DB_PATH}")
    print("NOTE: For telegram testing (7/8), both containers must be RUNNING.\n")

    while True:
        print("\n1) Add alert    2) Batch alert     3) Clear one    4) Clear all")
        print("5) Show active  6) Exit            7) Inject telegram msg")
        print("8) Show intel   9) Simulate UAV   10) Clear UAV tracks")
        choice = input("> ").strip()

        if choice == "1":
            cmd_add_alert(polygons)
        elif choice == "2":
            cmd_batch_alert(polygons)
        elif choice == "3":
            cmd_clear_one(polygons)
        elif choice == "4":
            cmd_clear_all()
        elif choice == "5":
            cmd_show_active()
        elif choice == "6":
            break
        elif choice == "7":
            cmd_telegram_inject()
        elif choice == "8":
            cmd_telegram_show()
        elif choice == "9":
            cmd_simulate_uav(polygons)
        elif choice == "10":
            cmd_clear_uav_tracks()
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
