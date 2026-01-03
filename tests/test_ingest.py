from datetime import date
import json

from asteroid_analysis import ingest


def test_chunking_inclusive():
    start = date(2024, 1, 1)
    end = date(2024, 1, 8)
    chunks = list(ingest.chunk_date_ranges(start, end))
    assert chunks == [
        (date(2024, 1, 1), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 8)),
    ]


def test_cache_skip_logic(tmp_path):
    raw_dir = tmp_path / "data/raw"
    raw_dir.mkdir(parents=True)
    cache_path = raw_dir / "feed_2024-01-01_2024-01-07.json"
    cache_path.write_text(json.dumps({"near_earth_objects": {}}))

    calls = {"count": 0}

    def fake_fetcher(session, start_date, end_date, api_key):
        calls["count"] += 1
        return {}

    ingest.fetch_or_load_chunk(
        session=None,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 7),
        api_key="demo",
        raw_dir=raw_dir,
        refresh=False,
        fetcher=fake_fetcher,
    )
    assert calls["count"] == 0

    ingest.fetch_or_load_chunk(
        session=None,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 7),
        api_key="demo",
        raw_dir=raw_dir,
        refresh=True,
        fetcher=fake_fetcher,
    )
    assert calls["count"] == 1


def test_schema_columns_present(tmp_path):
    raw_dir = tmp_path / "data/raw"
    raw_dir.mkdir(parents=True)
    cache_path = raw_dir / "feed_2024-01-01_2024-01-07.json"

    sample = {
        "near_earth_objects": {
            "2024-01-01": [
                {
                    "id": "1",
                    "neo_reference_id": "1",
                    "name": "Test",
                    "nasa_jpl_url": "http://example.com",
                    "absolute_magnitude_h": 22.0,
                    "is_potentially_hazardous_asteroid": True,
                    "is_sentry_object": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.1,
                            "estimated_diameter_max": 0.2,
                        },
                        "meters": {
                            "estimated_diameter_min": 100.0,
                            "estimated_diameter_max": 200.0,
                        },
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2024-01-01",
                            "close_approach_date_full": "2024-01-01 00:00",
                            "epoch_date_close_approach": 1704067200000,
                            "relative_velocity": {
                                "kilometers_per_second": "12.3",
                                "kilometers_per_hour": "44280",
                                "miles_per_hour": "27514",
                            },
                            "miss_distance": {
                                "astronomical": "0.01",
                                "lunar": "3.9",
                                "kilometers": "1500000",
                                "miles": "932000",
                            },
                            "orbiting_body": "Earth",
                        }
                    ],
                }
            ]
        }
    }
    cache_path.write_text(json.dumps(sample))

    df = ingest.build_dataframe_from_cache([cache_path], "Earth")
    assert list(df.columns) == ingest.SCHEMA_COLUMNS


def test_corrupt_cache_refetch_and_log(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    cache_path = raw_dir / "feed_2024-01-01_2024-01-07.json"
    cache_path.write_text("{bad json")

    calls = {"count": 0}

    def fake_fetcher(session, start_date, end_date, api_key):
        calls["count"] += 1
        return {"near_earth_objects": {}}

    result = ingest.fetch_or_load_chunk(
        session=None,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 7),
        api_key="demo",
        raw_dir=raw_dir,
        refresh=False,
        fetcher=fake_fetcher,
    )

    assert calls["count"] == 1
    assert result == cache_path
    failures_path = raw_dir / "failures.csv"
    assert failures_path.exists()


def test_invalid_schema_refetch_and_log(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    cache_path = raw_dir / "feed_2024-01-01_2024-01-07.json"
    cache_path.write_text(json.dumps({"foo": "bar"}))

    calls = {"count": 0}

    def fake_fetcher(session, start_date, end_date, api_key):
        calls["count"] += 1
        return {"near_earth_objects": {}}

    result = ingest.fetch_or_load_chunk(
        session=None,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 7),
        api_key="demo",
        raw_dir=raw_dir,
        refresh=False,
        fetcher=fake_fetcher,
    )

    assert calls["count"] == 1
    assert result == cache_path
    failures_path = raw_dir / "failures.csv"
    assert failures_path.exists()


def test_good_cache_skip_fetch(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    cache_path = raw_dir / "feed_2024-01-01_2024-01-07.json"
    cache_path.write_text(json.dumps({"near_earth_objects": {}}))

    calls = {"count": 0}

    def fake_fetcher(session, start_date, end_date, api_key):
        calls["count"] += 1
        return {"near_earth_objects": {}}

    result = ingest.fetch_or_load_chunk(
        session=None,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 7),
        api_key="demo",
        raw_dir=raw_dir,
        refresh=False,
        fetcher=fake_fetcher,
    )

    assert calls["count"] == 0
    assert result == cache_path
