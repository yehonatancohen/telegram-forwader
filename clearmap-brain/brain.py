"""
Clearmap Brain — Real-time Oref alert poller + Firebase sync.

Polls the Israeli Home Front Command API every 1.5s, manages a state machine
for each alerted city (active → waiting → removed), and pushes the current
state to Firebase Realtime Database for the Next.js frontend to consume.
"""

import json
import logging
import os
import time
from pathlib import Path

import firebase_admin
import requests
from firebase_admin import credentials, db

# ── Config ──────────────────────────────────────────────────────────────────

OREF_URL = "https://www.oref.org.il/WarningMessages/alert/alerts.json"
OREF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.oref.org.il/12481-he/PikudHaoref.aspx",
    "X-Requested-With": "XMLHttpRequest",
}

FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app/"
FIREBASE_NODE = "/public_state/active_alerts"

POLL_INTERVAL = 1.5       # seconds between API polls
SHELTER_TTL = 600          # 10 minutes in seconds
REQUEST_TIMEOUT = 5        # HTTP timeout in seconds

POLYGONS_FILE = Path(__file__).parent / "polygons.json"
SERVICE_ACCOUNT_FILE = Path(__file__).parent / "serviceAccountKey.json"

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("brain")

# ── Types ───────────────────────────────────────────────────────────────────


class CityState:
    """Internal state for a single alerted city."""

    __slots__ = ("state", "started_at", "is_double", "city_name", "city_name_he")

    def __init__(self, city_name_he: str, city_name: str, started_at: float):
        self.state = "active"
        self.started_at = started_at
        self.is_double = False
        self.city_name_he = city_name_he
        self.city_name = city_name

    def to_firebase(self) -> dict:
        """Serialize to the shape the frontend expects (ActiveAlert interface)."""
        return {
            "id": f"alert_{self.city_name_he}",
            "city_name": self.city_name,
            "city_name_he": self.city_name_he,
            "timestamp": int(self.started_at * 1000),  # JS milliseconds
            "is_double": self.is_double,
        }


# ── Polygon Lookup ──────────────────────────────────────────────────────────


def load_polygons() -> dict:
    """Load the Hebrew-city-name → polygon lookup from polygons.json."""
    if not POLYGONS_FILE.exists():
        log.error("polygons.json not found! Run fetch_polygons.py first.")
        return {}

    with open(POLYGONS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    log.info("Loaded %d municipality polygons.", len(data))
    return data


# ── Firebase Init ───────────────────────────────────────────────────────────


def init_firebase():
    """Initialize Firebase Admin SDK."""
    cred = credentials.Certificate(str(SERVICE_ACCOUNT_FILE))
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})
    log.info("Firebase initialized → %s", FIREBASE_DB_URL)


# ── Oref Polling ────────────────────────────────────────────────────────────


def fetch_oref() -> list[str]:
    """
    Fetch the current alert list from Oref.
    Returns a list of Hebrew city names currently under alert.
    """
    resp = requests.get(OREF_URL, headers=OREF_HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    text = resp.text.strip()

    # Remove BOM if present
    if text.startswith("\ufeff"):
        text = text[1:]

    text = text.strip()
    if not text or text in ("null", "[]", "{}"):
        return []

    data = json.loads(text)

    # Oref response has a 'data' field with list of city names
    if isinstance(data, dict):
        cities = data.get("data", [])
    elif isinstance(data, list):
        cities = data
    else:
        return []

    # Each item might be a string (city name) or a dict with 'data' field
    result = []
    for item in cities:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            name = item.get("data", "") or item.get("title", "")
            if name:
                result.append(name)

    return result


# ── State Machine ───────────────────────────────────────────────────────────


def update_state(
    state: dict[str, CityState],
    oref_cities: list[str],
    polygons: dict,
) -> bool:
    """
    Update the internal state dict based on the current Oref response.
    Returns True if the state changed (needs Firebase sync).
    """
    now = time.time()
    changed = False
    active_set = set(oref_cities)

    # A. Process cities IN the Oref response
    for city_he in oref_cities:
        if city_he in state:
            # City already tracked — mark as double salvo
            if not state[city_he].is_double:
                state[city_he].is_double = True
                changed = True
            state[city_he].started_at = now
            state[city_he].state = "active"
            changed = True
        else:
            # New city — look up polygon
            poly_data = polygons.get(city_he)
            if not poly_data:
                log.warning("No polygon data for '%s' — skipping.", city_he)
                continue

            state[city_he] = CityState(
                city_name_he=city_he,
                city_name=poly_data["city_name"],
                started_at=now,
            )
            log.info("🔴 NEW ALERT: %s (%s)", city_he, poly_data["city_name"])
            changed = True

    # B. Cities in our state but NOT in Oref → move to "waiting"
    for city_he, cs in state.items():
        if city_he not in active_set and cs.state == "active":
            cs.state = "waiting"
            log.info("⏳ WAITING: %s (shelter for 10 min)", city_he)
            changed = True

    # C. Cleanup: remove cities in "waiting" past the 10-min TTL
    expired = [
        city_he
        for city_he, cs in state.items()
        if cs.state == "waiting" and (now - cs.started_at) >= SHELTER_TTL
    ]
    for city_he in expired:
        log.info("✅ CLEARED: %s (10 min elapsed)", city_he)
        del state[city_he]
        changed = True

    return changed


# ── Firebase Sync ───────────────────────────────────────────────────────────


def sync_to_firebase(state: dict[str, CityState]):
    """Push the current state to Firebase Realtime Database."""
    ref = db.reference(FIREBASE_NODE)

    if not state:
        ref.set({})
        log.info("Firebase synced: no active alerts.")
        return

    payload = {city_he: cs.to_firebase() for city_he, cs in state.items()}
    ref.set(payload)
    log.info("Firebase synced: %d alerts.", len(payload))


# ── Main Loop ───────────────────────────────────────────────────────────────


def main():
    log.info("=== Clearmap Brain starting ===")

    polygons = load_polygons()
    if not polygons:
        log.error("Cannot start without polygon data. Exiting.")
        return

    init_firebase()

    state: dict[str, CityState] = {}

    # Clear any stale data on startup
    sync_to_firebase(state)

    log.info("Polling Oref every %.1fs (shelter TTL: %ds)...", POLL_INTERVAL, SHELTER_TTL)

    while True:
        try:
            oref_cities = fetch_oref()
            changed = update_state(state, oref_cities, polygons)

            if changed:
                sync_to_firebase(state)

        except requests.exceptions.RequestException as e:
            log.error("HTTP error: %s", e)
        except json.JSONDecodeError as e:
            log.error("JSON parse error: %s", e)
        except Exception as e:
            log.error("Unexpected error: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
