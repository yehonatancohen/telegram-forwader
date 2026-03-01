#!/usr/bin/env python3
"""
Interactive test tool — write mock alerts to Firebase RTDB.
Stop brain.py before using this to avoid conflicts.

Usage:
    python test_alerts.py
"""

import json
import re
import time
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, db

FIREBASE_DB_URL = "https://clear-map-f20d0-default-rtdb.europe-west1.firebasedatabase.app/"
FIREBASE_NODE = "/public_state/active_alerts"
SERVICE_ACCOUNT = Path(__file__).parent / "serviceAccountKey.json"
POLYGONS_FILE = Path(__file__).parent / "polygons.json"

STATUSES = ["alert", "pre_alert", "after_alert", "telegram_yellow", "uav", "terrorist"]


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


def main():
    # Load polygons
    with open(POLYGONS_FILE, encoding="utf-8") as f:
        polygons = json.load(f)

    # Init Firebase
    cred = credentials.Certificate(str(SERVICE_ACCOUNT))
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})

    print(f"Loaded {len(polygons)} cities.")
    print("WARNING: Stop brain.py before testing to avoid conflicts.\n")

    while True:
        print("\n1) Add alert   2) Batch alert   3) Clear one   4) Clear all   5) Show active   6) Exit")
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
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
