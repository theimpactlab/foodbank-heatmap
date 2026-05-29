#!/usr/bin/env python3
"""Offline helper to enrich city_coordinates.json from unmatched_cities.json.

This is intentionally separate from the nightly fetch. It resolves a small batch
of unmatched Google Trends place names through Nominatim, updates the coordinate
cache, and removes resolved names from the unmatched report so the backlog trends
towards zero over time.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

USER_AGENT = "FoodBankViz/1.0 (ryan@theimpactlab.co.uk)"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def geocode(name: str, nation_hint: str) -> dict[str, Any] | None:
    query = f"{name}, {nation_hint}, United Kingdom"
    params = urllib.parse.urlencode({
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "gb",
        "addressdetails": 1,
    })
    req = urllib.request.Request(
        f"{NOMINATIM_URL}?{params}",
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=20) as res:
        results = json.load(res)
    if not results:
        return None
    row = results[0]
    return {
        "nation": nation_hint,
        "lat": round(float(row["lat"]), 6),
        "lng": round(float(row["lon"]), 6),
        "aliases": [],
    }


def write_unmatched(path: Path, original_payload: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    payload = dict(original_payload)
    payload["schema"] = payload.get("schema") or "foodbank-heatmap.unmatched_cities.v1"
    payload["generated_at"] = payload.get("generated_at")
    payload["last_enriched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload["count"] = len(rows)
    payload["cities"] = rows
    path.write_text(json.dumps(payload, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich city coordinate cache from unmatched city names")
    parser.add_argument("--coordinates", default="data/city_coordinates.json")
    parser.add_argument("--unmatched", default="data/unmatched_cities.json")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=1.2, help="Delay between Nominatim requests")
    args = parser.parse_args()

    coords_path = Path(args.coordinates)
    unmatched_path = Path(args.unmatched)
    coords = load_json(coords_path, {"schema": "foodbank-heatmap.city_coordinates.v1", "cities": {}})
    cities = coords.setdefault("cities", {})
    unmatched_payload = load_json(unmatched_path, {"schema": "foodbank-heatmap.unmatched_cities.v1", "cities": []})
    unmatched = unmatched_payload.get("cities") or []

    added_names: set[str] = set()
    failed: list[str] = []
    attempted = 0

    for row in unmatched:
        if attempted >= args.limit:
            break
        name = row.get("name")
        nation_hint = row.get("nation_hint") or "United Kingdom"
        if not name or name in cities:
            if name:
                added_names.add(name)
            continue

        attempted += 1
        try:
            result = geocode(name, nation_hint)
        except Exception as exc:  # noqa: BLE001 - CLI should keep going
            print(f"FAIL {name}: {exc}")
            failed.append(name)
            time.sleep(args.delay)
            continue
        if not result:
            print(f"MISS {name}")
            failed.append(name)
            time.sleep(args.delay)
            continue

        print(f"ADD {name}: {result['lat']}, {result['lng']} ({result['nation']})")
        cities[name] = result
        added_names.add(name)
        time.sleep(args.delay)

    remaining = [row for row in unmatched if row.get("name") not in added_names]

    if not args.dry_run and added_names:
        coords["cities"] = dict(sorted(cities.items()))
        coords_path.write_text(json.dumps(coords, indent=2, sort_keys=True))
        write_unmatched(unmatched_path, unmatched_payload, remaining)
        print(f"Wrote {coords_path} with {len(coords['cities'])} coordinates")
        print(f"Updated {unmatched_path}: {len(remaining)} unmatched remain")
    else:
        print(f"No write. added={len(added_names)} attempted={attempted} dry_run={args.dry_run}")

    if failed:
        print(f"Unresolved this run: {len(failed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
