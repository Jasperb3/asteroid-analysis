import json

import pandas as pd

from asteroid_analysis import enrich_orbits


def test_orbit_cache_skip(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    cache_path = raw_dir / "neo_123.json"
    cache_path.write_text("{}")

    calls = {"count": 0}

    def fake_fetcher(session, neo_id, api_key):
        calls["count"] += 1
        return {}

    enrich_orbits.fetch_or_load_orbit(
        session=None,
        neo_id="123",
        api_key="demo",
        raw_dir=raw_dir,
        refresh=False,
        fetcher=fake_fetcher,
    )
    assert calls["count"] == 0

    enrich_orbits.fetch_or_load_orbit(
        session=None,
        neo_id="123",
        api_key="demo",
        raw_dir=raw_dir,
        refresh=True,
        fetcher=fake_fetcher,
    )
    assert calls["count"] == 1


def test_orbit_schema_and_missing_fields(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    cache_path = raw_dir / "neo_999.json"
    payload = {
        "id": "999",
        "orbital_data": {
            "orbit_id": "orbit-1",
            "orbit_class": {"orbit_class_name": "Apollo"},
        },
    }
    cache_path.write_text(json.dumps(payload))

    df = pd.DataFrame([enrich_orbits.extract_orbit_fields(payload)])
    assert list(df.columns) == enrich_orbits.ORBIT_COLUMNS

    missing_payload = {"id": "1000"}
    df_missing = pd.DataFrame([enrich_orbits.extract_orbit_fields(missing_payload)])
    assert list(df_missing.columns) == enrich_orbits.ORBIT_COLUMNS
