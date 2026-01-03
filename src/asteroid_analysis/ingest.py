import argparse
import csv
import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm


FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed"
RAW_DIR = Path("data/raw")

SCHEMA_COLUMNS = [
    "date",
    "id",
    "neo_reference_id",
    "name",
    "nasa_jpl_url",
    "absolute_magnitude_h",
    "is_potentially_hazardous_asteroid",
    "is_sentry_object",
    "diameter_km_min",
    "diameter_km_max",
    "diameter_m_min",
    "diameter_m_max",
    "close_approach_date",
    "close_approach_date_full",
    "epoch_date_close_approach",
    "velocity_km_s",
    "velocity_km_h",
    "velocity_mph",
    "miss_distance_astronomical",
    "miss_distance_lunar",
    "miss_distance_km",
    "miss_distance_miles",
    "orbiting_body",
]


def chunk_date_ranges(start_date: date, end_date: date, days: int = 7):
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def fetch_chunk(session: requests.Session, start_date: date, end_date: date, api_key: str):
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "api_key": api_key,
    }
    max_retries = 5
    for attempt in range(max_retries + 1):
        try:
            response = session.get(FEED_URL, params=params, timeout=30)
        except requests.RequestException as exc:
            if attempt == max_retries:
                raise RuntimeError(f"Request error: {exc}") from exc
            time.sleep(2**attempt)
            continue

        if response.status_code == 200:
            return response.json()
        if response.status_code == 429 or 500 <= response.status_code < 600:
            if attempt == max_retries:
                raise RuntimeError(
                    f"API error {response.status_code}: {response.text}"
                )
            time.sleep(2**attempt)
            continue
        raise RuntimeError(f"API error {response.status_code}: {response.text}")


def _write_failure(
    raw_dir: Path, start_date: date | None, end_date: date | None, error: str
):
    failures_path = raw_dir / "failures.csv"
    write_header = not failures_path.exists()
    with failures_path.open("a", newline="") as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(["start_date", "end_date", "error"])
        start_value = start_date.isoformat() if start_date else ""
        end_value = end_date.isoformat() if end_date else ""
        writer.writerow([start_value, end_value, error])


def _parse_chunk_range_from_path(cache_path: Path):
    name = cache_path.stem
    if not name.startswith("feed_"):
        return None, None
    parts = name.split("_")
    if len(parts) != 3:
        return None, None
    try:
        start_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
        end_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
    except ValueError:
        return None, None
    return start_date, end_date


def _validate_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Invalid payload type")
    if "near_earth_objects" not in payload or not isinstance(
        payload.get("near_earth_objects"), dict
    ):
        raise ValueError("Missing or invalid near_earth_objects")


def _read_cache_payload(
    cache_path: Path, raw_dir: Path, start_date: date | None, end_date: date | None
):
    try:
        payload = json.loads(cache_path.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        error = f"Corrupt cache JSON {cache_path}: {exc}"
        print(error)
        _write_failure(raw_dir, start_date, end_date, error)
        return None

    try:
        _validate_payload(payload)
    except ValueError as exc:
        error = f"Invalid cache schema {cache_path}: {exc}"
        print(error)
        _write_failure(raw_dir, start_date, end_date, error)
        return None

    return payload


def _write_cache_atomic(cache_path: Path, payload: dict):
    temp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload))
    temp_path.replace(cache_path)


def fetch_or_load_chunk(
    session: requests.Session,
    start_date: date,
    end_date: date,
    api_key: str,
    raw_dir: Path,
    refresh: bool,
    fetcher=fetch_chunk,
):
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_path = raw_dir / f"feed_{start_date.isoformat()}_{end_date.isoformat()}.json"
    if cache_path.exists() and not refresh:
        cached_payload = _read_cache_payload(cache_path, raw_dir, start_date, end_date)
        if cached_payload is not None:
            return cache_path

    try:
        payload = fetcher(session, start_date, end_date, api_key)
        _validate_payload(payload)
    except Exception as exc:
        print(f"Failed chunk {start_date}..{end_date}: {exc}")
        _write_failure(raw_dir, start_date, end_date, str(exc))
        return None

    _write_cache_atomic(cache_path, payload)
    return cache_path


def build_dataframe_from_cache(cache_paths, orbiting_body: str) -> pd.DataFrame:
    rows = []
    include_all = orbiting_body.lower() == "all"

    for cache_path in cache_paths:
        if cache_path is None or not Path(cache_path).exists():
            continue
        cache_path = Path(cache_path)
        start_date, end_date = _parse_chunk_range_from_path(cache_path)
        data = _read_cache_payload(cache_path, RAW_DIR, start_date, end_date)
        if data is None:
            continue
        neo_map = data.get("near_earth_objects", {})

        for _, asteroids in neo_map.items():
            for asteroid in asteroids:
                est_diam = asteroid.get("estimated_diameter", {})
                km = est_diam.get("kilometers", {})
                m = est_diam.get("meters", {})
                approaches = asteroid.get("close_approach_data", []) or []
                for approach in approaches:
                    orbit_body = approach.get("orbiting_body")
                    if not include_all and orbit_body != orbiting_body:
                        continue

                    close_date = approach.get("close_approach_date")
                    row = {
                        "date": close_date,
                        "id": asteroid.get("id"),
                        "neo_reference_id": asteroid.get("neo_reference_id"),
                        "name": asteroid.get("name"),
                        "nasa_jpl_url": asteroid.get("nasa_jpl_url"),
                        "absolute_magnitude_h": asteroid.get("absolute_magnitude_h"),
                        "is_potentially_hazardous_asteroid": asteroid.get(
                            "is_potentially_hazardous_asteroid"
                        ),
                        "is_sentry_object": asteroid.get("is_sentry_object"),
                        "diameter_km_min": km.get("estimated_diameter_min"),
                        "diameter_km_max": km.get("estimated_diameter_max"),
                        "diameter_m_min": m.get("estimated_diameter_min"),
                        "diameter_m_max": m.get("estimated_diameter_max"),
                        "close_approach_date": close_date,
                        "close_approach_date_full": approach.get(
                            "close_approach_date_full"
                        ),
                        "epoch_date_close_approach": approach.get(
                            "epoch_date_close_approach"
                        ),
                        "velocity_km_s": approach.get("relative_velocity", {}).get(
                            "kilometers_per_second"
                        ),
                        "velocity_km_h": approach.get("relative_velocity", {}).get(
                            "kilometers_per_hour"
                        ),
                        "velocity_mph": approach.get("relative_velocity", {}).get(
                            "miles_per_hour"
                        ),
                        "miss_distance_astronomical": approach.get("miss_distance", {}).get(
                            "astronomical"
                        ),
                        "miss_distance_lunar": approach.get("miss_distance", {}).get(
                            "lunar"
                        ),
                        "miss_distance_km": approach.get("miss_distance", {}).get(
                            "kilometers"
                        ),
                        "miss_distance_miles": approach.get("miss_distance", {}).get(
                            "miles"
                        ),
                        "orbiting_body": orbit_body,
                    }
                    rows.append(row)

    df = pd.DataFrame(rows, columns=SCHEMA_COLUMNS)
    numeric_cols = [
        "absolute_magnitude_h",
        "diameter_km_min",
        "diameter_km_max",
        "diameter_m_min",
        "diameter_m_max",
        "epoch_date_close_approach",
        "velocity_km_s",
        "velocity_km_h",
        "velocity_mph",
        "miss_distance_astronomical",
        "miss_distance_lunar",
        "miss_distance_km",
        "miss_distance_miles",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def ingest(
    start_date: date,
    end_date: date,
    orbiting_body: str,
    out_path: Path,
    refresh: bool,
    raw_dir: Path = RAW_DIR,
):
    api_key = os.getenv("NASA_API_KEY")
    if not api_key:
        raise RuntimeError("NASA_API_KEY environment variable not set.")

    chunks = list(chunk_date_ranges(start_date, end_date))
    cache_paths = []
    with requests.Session() as session:
        for chunk_start, chunk_end in tqdm(chunks, desc="Fetching chunks"):
            cache_path = fetch_or_load_chunk(
                session=session,
                start_date=chunk_start,
                end_date=chunk_end,
                api_key=api_key,
                raw_dir=raw_dir,
                refresh=refresh,
            )
            cache_paths.append(cache_path)

    df = build_dataframe_from_cache(cache_paths, orbiting_body)
    df.to_csv(out_path, index=False)
    print(f"Full dataset saved to: {out_path}")
    return df


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(description="Fetch and cache NeoWs feed data.")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive).")
    parser.add_argument("--end", help="End date YYYY-MM-DD (inclusive).")
    parser.add_argument(
        "--orbiting-body",
        default="Earth",
        help="Orbiting body filter (use 'all' for every body).",
    )
    parser.add_argument(
        "--out",
        default="asteroid_data_full.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-fetch cached chunks.",
    )
    args = parser.parse_args()

    if args.start and args.end:
        start_date = _parse_date(args.start)
        end_date = _parse_date(args.end)
    else:
        start_date = datetime.now().date()
        end_date = start_date + timedelta(days=365 * 15)

    ingest(
        start_date=start_date,
        end_date=end_date,
        orbiting_body=args.orbiting_body,
        out_path=Path(args.out),
        refresh=args.refresh,
    )


if __name__ == "__main__":
    main()
