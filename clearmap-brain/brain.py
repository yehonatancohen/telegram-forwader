"""
Clearmap Brain — Real-time Oref alert poller + Telegram intel + Firebase sync.

Polls the Israeli Home Front Command API every 1.5s, manages a state machine
for each alerted city, reads Telegram intel from intel.db, and pushes the
current state to Firebase Realtime Database for the Next.js frontend.

State machine:
  telegram_yellow → pre_alert → alert → after_alert → (removed)
  
Timings:
  telegram_yellow: 2 min max, or until pre_alert arrives
  pre_alert:       12 min max, or until alert arrives
  alert:           1.5 min, then auto-transitions to after_alert
  after_alert:     persists until Oref clearance ("הסתיים"/"ניתן לצאת")
"""

import json
import logging
import os
import re
import sys
import sqlite3
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

POLL_INTERVAL = 1.5        # seconds between API polls
REQUEST_TIMEOUT = 5        # HTTP timeout in seconds

# ── Timing constants ────────────────────────────────────────────────────────
TELEGRAM_YELLOW_TTL = 120   # 2 minutes
PRE_ALERT_TTL = 720         # 12 minutes
ALERT_DURATION = 90         # 1.5 minutes → then after_alert
AFTER_ALERT_SAFETY_TTL = 86400  # 24h safety net (cleared by Oref signal normally)

POLYGONS_FILE = Path(os.environ.get("POLYGONS_FILE", Path(__file__).parent / "polygons.json"))
SERVICE_ACCOUNT_FILE = Path(os.environ.get("SERVICE_ACCOUNT_FILE", Path(__file__).parent / "serviceAccountKey.json"))
TELEGRAM_DB_PATH = Path(os.environ.get("TELEGRAM_DB_PATH", Path(__file__).parent.parent / "data" / "intel.db"))

# ── Telegram intel config ───────────────────────────────────────────────────
# Channels that brain reads from the intel DB for yellow highlighting.
INTEL_CHANNELS = {"beforeredalert", "yemennews7071"}

# Keywords that indicate incoming alerts → mark location yellow
INTEL_KEYWORDS = ["יציאות", "שיגורים", "להתמגן", "שיגור", "טילים", "רקטות"]

# Keywords that indicate alert is OVER (people can leave shelters) → skip message
INTEL_CANCEL_KEYWORDS = [
    "ניתן לצאת",        # "you can leave"
    "הותר לצאת",        # "permitted to leave"
    "איתות ירוק",       # "green signal"
    "הסתיים",           # "ended"
    "בוטלה",            # "cancelled" (fem)
    "בוטל",             # "cancelled" (masc)
    "אין איום",         # "no threat"
]

# Region name → list of district names for area mapping
REGION_MAPPING = {
    "צפון": ["מחוז צפון", "מחוז גליל עליון", "מחוז גליל תחתון", "מחוז גולן",
             "מחוז חיפה", "מחוז יערות הכרמל", "מחוז קו העימות", "מחוז העמקים",
             "מחוז בקעת בית שאן"],
    "דרום": ["מחוז דרום הנגב", "מחוז עוטף עזה", "מחוז נגב", "מחוז דרום השפלה",
             "מחוז אילת", "מחוז ים המלח"],
    "מרכז": ["מחוז דן", "מחוז השפלה", "מחוז ירקון", "מחוז שרון", "מחוז חפר",
             "מחוז שומרון"],
    "שפלה": ["מחוז השפלה", "מחוז דרום השפלה"],
    "נגב":  ["מחוז דרום הנגב", "מחוז נגב"],
    "ירושלים": ["מחוז ירושלים", "מחוז יהודה", "מחוז בית שמש", "מחוז בקעה"],
}

# Keyword → region for quick matching
REGION_KEYWORDS = {
    "צפון": "צפון", "גליל": "צפון", "גולן": "צפון", "חיפה": "צפון",
    "דרום": "דרום", "עוטף": "דרום", "נגב": "דרום", "אשקלון": "דרום",
    "מרכז": "מרכז", "דן": "מרכז", "שרון": "מרכז", "גוש דן": "מרכז",
    "שפלה": "שפלה",
    "ירושלים": "ירושלים", "יו\"ש": "ירושלים", "יהודה": "ירושלים", "שומרון": "מרכז",
}

# ── Import District Metadata ────────────────────────────────────────────────
try:
    from district_to_areas import DISTRICT_AREAS
except ImportError:
    DISTRICT_AREAS = {}

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
        self.state = "alert"
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
            "status": self.state,
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


def _classify_alert_object(alert_obj: dict) -> str:
    """Classify a single Oref alert object into a status string.

    Based on real API samples:
    - cat "1"  + "ירי רקטות וטילים"                     → "alert"
    - cat "1"  + "חדירת כלי טיס עוין"                   → "uav"
    - cat "1"  + "חדירת מחבלים"                          → "terrorist"
    - cat "10" + "בדקות הקרובות צפויות להתקבל התרעות"   → "pre_alert"
    - cat "10" + clearance signals                       → "clear"
    """
    cat = str(alert_obj.get("cat", ""))
    title = alert_obj.get("title", "")

    # Clearance signals → remove from map (check before cat "1" since
    # "חדירת כלי טיס עוין - האירוע הסתיים" contains both keywords)
    if "ניתן לצאת" in title or "להישאר בקרבת" in title or "הסתיים" in title or "החשש הוסר" in title:
        return "clear"

    if cat == "1":
        if "חדירת כלי טיס עוין" in title:
            return "uav"
        if "חדירת מחבלים" in title:
            return "terrorist"
        return "alert"

    if "בדקות הקרובות" in title or "שהייה בסמיכות" in title or "לשפר את המיקום" in title:
        return "pre_alert"

    # Default: treat unknown cat values as alert to be safe
    return "alert"


# Priority: higher number = takes precedence when merging
_STATUS_PRIORITY = {"telegram_yellow": 0, "after_alert": 1, "pre_alert": 2, "alert": 3, "uav": 3, "terrorist": 3}


def fetch_oref() -> list[tuple[str, str]]:
    """
    Fetch the current alert list from Oref.
    
    The API returns an array of alert objects, each with:
      - cat: category string ("1" for rockets, "10" for advisories)
      - title: descriptive Hebrew text
      - data: list of city name strings
    
    Returns a list of (city_name_he, status) tuples with priority merging.
    """
    resp = requests.get(OREF_URL, headers=OREF_HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    text = resp.text.strip()
    if text.startswith("\ufeff"):
        text = text[1:]
    text = text.strip()
    if not text or text in ("null", "[]", "{}"):
        return []

    data = json.loads(text)

    # Normalise: API can return a single object or an array of objects
    if isinstance(data, dict):
        alerts = [data]
    elif isinstance(data, list):
        # Could be a list of alert objects or (legacy) a flat list of city strings
        if data and isinstance(data[0], str):
            return [(city, "alert") for city in data]
        alerts = data
    else:
        return []

    # Merge cities from all alert objects with priority
    merged: dict[str, str] = {}

    for alert_obj in alerts:
        if not isinstance(alert_obj, dict):
            continue
        status = _classify_alert_object(alert_obj)
        cities = alert_obj.get("data", [])
        
        log.debug("Oref object: cat=%s title='%s' status=%s cities=%d",
                   alert_obj.get("cat"), alert_obj.get("title", "")[:40], status, len(cities))

        for city in cities:
            if not isinstance(city, str):
                continue
            existing = merged.get(city)
            if existing is None or _STATUS_PRIORITY.get(status, 0) > _STATUS_PRIORITY.get(existing, 0):
                merged[city] = status

    return list(merged.items())


# ── Telegram Intel ──────────────────────────────────────────────────────────


def _resolve_region(region_key: str) -> list[str]:
    """Given a region keyword, return all matching city names."""
    if not DISTRICT_AREAS:
        return []
    
    districts = REGION_MAPPING.get(region_key, [])
    cities = []
    for dist in districts:
        cities.extend(DISTRICT_AREAS.get(dist, []))
    return cities


def _extract_locations_from_text(text: str, polygons: dict) -> set[str]:
    """Extract city/region names from a Telegram message text."""
    found = set()
    
    # Check for region keywords
    for keyword, region in REGION_KEYWORDS.items():
        if keyword in text:
            region_cities = _resolve_region(region)
            found.update(region_cities)
    
    # Check for direct city name matches (only check if text is short enough - likely a location)
    # For longer messages, rely on keywords and regions
    if len(text) < 200:
        for city_he in polygons:
            if city_he in text and len(city_he) > 2:
                found.add(city_he)
    
    return found


def fetch_telegram_alerts(polygons: dict) -> list[tuple[str, str]]:
    """
    Read recent messages from intel DB channels (beforeredalert, yemennews7071).
    Look for keywords indicating incoming alerts and extract location info.
    Returns a list of (city_name_he, "telegram_yellow") tuples.
    """
    if not TELEGRAM_DB_PATH.exists():
        return []

    cities: set[str] = set()
    try:
        conn = sqlite3.connect(str(TELEGRAM_DB_PATH), timeout=2)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        now = time.time()
        two_min_ago = now - TELEGRAM_YELLOW_TTL

        cur.execute("""
            SELECT es.raw_text, es.channel
            FROM event_sources es
            WHERE es.reported_at >= ?
              AND LOWER(es.channel) IN ({})
            ORDER BY es.reported_at DESC
        """.format(",".join(f"'{c}'" for c in INTEL_CHANNELS)), (two_min_ago,))

        for row in cur.fetchall():
            raw = row["raw_text"] or ""
            if not raw:
                continue

            if any(ck in raw for ck in INTEL_CANCEL_KEYWORDS):
                continue

            if any(kw in raw for kw in INTEL_KEYWORDS):
                extracted = _extract_locations_from_text(raw, polygons)
                if extracted:
                    cities.update(extracted)

        conn.close()
    except Exception as e:
        log.error("Telegram DB error: %s", e)

    return [(c, "telegram_yellow") for c in cities]


# ── State Machine ───────────────────────────────────────────────────────────


def update_state(
    state: dict[str, CityState],
    oref_data: list[tuple[str, str]],
    telegram_data: list[tuple[str, str]],
    polygons: dict,
) -> bool:
    """
    Updates the internal state machine.
    
    State machine rules:
    - telegram_yellow: 2 min timeout, or upgraded to pre_alert/alert
    - pre_alert:       12 min timeout, or upgraded to alert
    - alert:           1.5 min duration, then auto → after_alert
    - after_alert:     persists until Oref "clear" signal. Re-alertable (→ alert)
    
    Priority: alert > pre_alert > after_alert > telegram_yellow
    """
    now = time.time()
    changed = False
    
    # ── Step 0: Time-based auto-transitions ──────────────────────────────
    for city_he, cs in list(state.items()):
        elapsed = now - cs.started_at
        
        if cs.state in ("alert", "uav", "terrorist") and elapsed >= ALERT_DURATION:
            # Alert expired → after_alert (shelter)
            cs.state = "after_alert"
            cs.started_at = now
            log.info("⏳ ALERT→AFTER_ALERT: %s (%.0fs elapsed)", city_he, elapsed)
            changed = True
        
        elif cs.state == "pre_alert" and elapsed >= PRE_ALERT_TTL:
            # Pre-alert expired without real alert → remove
            log.info("✅ PRE_ALERT EXPIRED: %s (%.0fs)", city_he, elapsed)
            del state[city_he]
            changed = True
        
        elif cs.state == "telegram_yellow" and elapsed >= TELEGRAM_YELLOW_TTL:
            # Telegram yellow expired → remove
            log.info("✅ TELEGRAM EXPIRED: %s (%.0fs)", city_he, elapsed)
            del state[city_he]
            changed = True
        
        elif cs.state == "after_alert" and elapsed >= AFTER_ALERT_SAFETY_TTL:
            # 24h safety net — normally cleared by Oref "הסתיים" signal
            log.warning("⚠️ SAFETY CLEANUP: %s after_alert for 24h without clearance", city_he)
            del state[city_he]
            changed = True
    
    # ── Step 1: Merge all incoming signals with priority ──────────────────
    incoming: dict[str, str] = {}
    
    # Telegram yellow = lowest priority
    for city_he, status in telegram_data:
        incoming[city_he] = status
    
    # Oref signals override telegram
    for city_he, status in oref_data:
        existing = incoming.get(city_he)
        if existing is None or _STATUS_PRIORITY.get(status, 0) > _STATUS_PRIORITY.get(existing, 0):
            incoming[city_he] = status
    
    oref_cities = {city for city, _ in oref_data}
    
    # ── Step 1b: Handle clearance signals (remove from map) ────────────
    for city_he, status in list(incoming.items()):
        if status == "clear":
            if city_he in state:
                log.info("✅ CLEARED by Oref: %s", city_he)
                del state[city_he]
                changed = True
            del incoming[city_he]

    # ── Step 2: Process incoming signals ──────────────────────────────────
    for city_he, alert_type in incoming.items():
        if city_he in state:
            cs = state[city_he]
            old_state = cs.state
            new_priority = _STATUS_PRIORITY.get(alert_type, 0)
            old_priority = _STATUS_PRIORITY.get(old_state, 0)
            
            # Only upgrade state (higher priority), never downgrade via incoming signal
            # Exception: after_alert can be re-promoted to alert
            if new_priority > old_priority or (old_state == "after_alert" and alert_type in ("alert", "uav", "terrorist")):
                log.info("🔄 %s → %s: %s", old_state, alert_type, city_he)
                cs.state = alert_type
                cs.started_at = now
                cs.is_double = (old_state in ("alert", "uav", "terrorist", "after_alert") and alert_type in ("alert", "uav", "terrorist"))
                changed = True
            elif old_state == alert_type and alert_type in ("alert", "uav", "terrorist", "pre_alert"):
                # Same state from Oref — refresh timer only for active oref states
                if city_he in oref_cities:
                    cs.started_at = now
        else:
            # New city
            poly_data = polygons.get(city_he)
            if not poly_data:
                if alert_type != "telegram_yellow":
                    log.warning("No polygon data for '%s' — skipping.", city_he)
                continue
            
            cs = CityState(city_he, poly_data["city_name"], now)
            cs.state = alert_type
            state[city_he] = cs
            
            emoji = {"telegram_yellow": "🟡", "pre_alert": "🟠", "alert": "🔴", "uav": "🟣", "terrorist": "🔶", "after_alert": "⚫"}.get(alert_type, "❓")
            log.info("%s NEW %s: %s (%s)", emoji, alert_type.upper(), city_he, poly_data["city_name"])
            changed = True
    
    # ── Step 3: Oref signals that STOPPED ─────────────────────────────────
    # If a city was in pre_alert/alert from Oref but is no longer in Oref response,
    # let the timer handle the transition (Step 0 on next tick).
    # We do NOT force-transition here — the Oref API might just be between polls.
    
    return changed


# ── Firebase Sync ───────────────────────────────────────────────────────────


def _sanitize_fb_key(key: str) -> str:
    """Sanitize a string for use as a Firebase RTDB key.

    Firebase keys cannot contain: . $ # [ ] /
    Replace them with underscores.
    """
    return re.sub(r'[.$/\[\]#]', '_', key)


def sync_to_firebase(state: dict[str, CityState]):
    """Push the current state to Firebase Realtime Database."""
    ref = db.reference(FIREBASE_NODE)

    if not state:
        ref.set({})
        log.info("Firebase synced: no active alerts.")
        return

    payload = {_sanitize_fb_key(city_he): cs.to_firebase() for city_he, cs in state.items()}
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

    log.info("Polling Oref every %.1fs | alert=%ds pre_alert=%ds after_alert=until_clear telegram=%ds",
             POLL_INTERVAL, ALERT_DURATION, PRE_ALERT_TTL, TELEGRAM_YELLOW_TTL)

    while True:
        try:
            oref_data = fetch_oref()
            telegram_data = fetch_telegram_alerts(polygons)
            changed = update_state(state, oref_data, telegram_data, polygons)

            if changed:
                sync_to_firebase(state)

        except requests.exceptions.RequestException as e:
            log.error("HTTP error: %s", e)
        except json.JSONDecodeError as e:
            log.error("JSON parse error: %s", e)
        except Exception as e:
            log.error("Unexpected error: %s", e, exc_info=True)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
