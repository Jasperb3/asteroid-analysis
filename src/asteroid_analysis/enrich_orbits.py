import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
import requests


LOOKUP_URL = "https://api.nasa.gov/neo/rest/v1/neo/{}"
RAW_DIR = Path("data/raw")

ORBIT_COLUMNS = [
    "id",
    "orbit_id",
    "orbit_class_name",
    "orbit_class_type",
    "orbit_class_description",
    "semi_major_axis",
    "eccentricity",
    "inclination",
    "perihelion_distance",
    "aphelion_distance",
    "minimum_orbit_intersection",
    "orbital_period",
    "mean_anomaly",
    "ascending_node_longitude",
    "perihelion_argument",
]


def fetch_orbit(session: requests.Session, neo_id: str, api_key: str):
    url = LOOKUP_URL.format(neo_id)
    params = {"api_key": api_key}
    max_retries = 5
    for attempt in range(max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=30)
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


def fetch_or_load_orbit(
    session: requests.Session,
    neo_id: str,
    api_key: str,
    raw_dir: Path,
    refresh: bool,
    fetcher=fetch_orbit,
):
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_path = raw_dir / f"neo_{neo_id}.json"
    if cache_path.exists() and not refresh:
        return cache_path

    payload = fetcher(session, neo_id, api_key)
    cache_path.write_text(json.dumps(payload))
    return cache_path


def extract_orbit_fields(payload: dict) -> dict:
    orbital = payload.get("orbital_data", {}) if isinstance(payload, dict) else {}
    orbit_class = orbital.get("orbit_class", {}) if isinstance(orbital, dict) else {}
    return {
        "id": payload.get("id"),
        "orbit_id": orbital.get("orbit_id"),
        "orbit_class_name": orbit_class.get("orbit_class_name"),
        "orbit_class_type": orbit_class.get("orbit_class_type"),
        "orbit_class_description": orbit_class.get("orbit_class_description"),
        "semi_major_axis": orbital.get("semi_major_axis"),
        "eccentricity": orbital.get("eccentricity"),
        "inclination": orbital.get("inclination"),
        "perihelion_distance": orbital.get("perihelion_distance"),
        "aphelion_distance": orbital.get("aphelion_distance"),
        "minimum_orbit_intersection": orbital.get("minimum_orbit_intersection"),
        "orbital_period": orbital.get("orbital_period"),
        "mean_anomaly": orbital.get("mean_anomaly"),
        "ascending_node_longitude": orbital.get("ascending_node_longitude"),
        "perihelion_argument": orbital.get("perihelion_argument"),
    }


def build_orbits(
    out_path: Path,
    objects_path: Path = Path("data/processed/objects.parquet"),
    raw_dir: Path = RAW_DIR,
    refresh: bool = False,
):
    api_key = os.getenv("NASA_API_KEY")
    if not api_key:
        raise RuntimeError("NASA_API_KEY environment variable not set.")

    objects = pd.read_parquet(objects_path)
    ids = objects["id"].astype(str).dropna().unique().tolist()

    rows = []
    with requests.Session() as session:
        for neo_id in ids:
            cache_path = fetch_or_load_orbit(
                session=session,
                neo_id=neo_id,
                api_key=api_key,
                raw_dir=raw_dir,
                refresh=refresh,
            )
            payload = json.loads(cache_path.read_text())
            rows.append(extract_orbit_fields(payload))

    df = pd.DataFrame(rows, columns=ORBIT_COLUMNS)
    numeric_cols = [
        "semi_major_axis",
        "eccentricity",
        "inclination",
        "perihelion_distance",
        "aphelion_distance",
        "minimum_orbit_intersection",
        "orbital_period",
        "mean_anomaly",
        "ascending_node_longitude",
        "perihelion_argument",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Enrich asteroid objects with orbit data from NeoWs."
    )
    parser.add_argument(
        "--out",
        default="data/processed/orbits.parquet",
        help="Output parquet path.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-fetch cached orbit data.",
    )
    args = parser.parse_args()

    build_orbits(Path(args.out), refresh=args.refresh)


if __name__ == "__main__":
    main()
