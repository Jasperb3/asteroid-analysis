import pandas as pd

from asteroid_analysis import build


def _sample_df():
    return pd.DataFrame(
        [
            {
                "date": "2029-04-13",
                "id": "1001",
                "neo_reference_id": "2001",
                "name": "Test A",
                "nasa_jpl_url": "http://example.com/a",
                "absolute_magnitude_h": 22.1,
                "is_potentially_hazardous_asteroid": True,
                "is_sentry_object": False,
                "diameter_km_min": 0.0,
                "diameter_km_max": 0.0,
                "diameter_m_min": 0.0,
                "diameter_m_max": 0.0,
                "close_approach_date": "2029-04-13",
                "close_approach_date_full": "2029-04-13 00:00",
                "epoch_date_close_approach": 1234567890,
                "velocity_km_s": 12.3,
                "velocity_km_h": 44280.0,
                "velocity_mph": 27514.0,
                "miss_distance_astronomical": 0.01,
                "miss_distance_lunar": 3.9,
                "miss_distance_km": 0.0,
                "miss_distance_miles": 0.0,
                "orbiting_body": "Earth",
            },
            {
                "date": "2029-04-14",
                "id": "1001",
                "neo_reference_id": "2001",
                "name": None,
                "nasa_jpl_url": None,
                "absolute_magnitude_h": None,
                "is_potentially_hazardous_asteroid": True,
                "is_sentry_object": False,
                "diameter_km_min": 0.1,
                "diameter_km_max": 0.2,
                "diameter_m_min": 100.0,
                "diameter_m_max": 200.0,
                "close_approach_date": "2029-04-14",
                "close_approach_date_full": "2029-04-14 00:00",
                "epoch_date_close_approach": 1234567999,
                "velocity_km_s": 10.0,
                "velocity_km_h": 36000.0,
                "velocity_mph": 22369.0,
                "miss_distance_astronomical": 0.02,
                "miss_distance_lunar": 7.8,
                "miss_distance_km": 50000.0,
                "miss_distance_miles": 31068.0,
                "orbiting_body": "Earth",
            },
        ]
    )


def test_approach_id_creation():
    objects, approaches = build.process_dataframe(_sample_df())
    assert "approach_id" in approaches.columns
    assert approaches.loc[0, "approach_id"] == "1001_1234567890"


def test_object_dedupe_count():
    objects, _ = build.process_dataframe(_sample_df())
    assert len(objects) == 1


def test_derived_columns_safe_on_zeros():
    objects, approaches = build.process_dataframe(_sample_df())
    assert "log_miss_distance_km" in approaches.columns
    assert "log_diameter_mid_km" in objects.columns
    assert pd.isna(approaches.loc[0, "log_miss_distance_km"])
    assert pd.isna(objects.loc[0, "log_diameter_mid_km"])
