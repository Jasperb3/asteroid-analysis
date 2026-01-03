from pathlib import Path

import pandas as pd

from asteroid_analysis import build
from asteroid_analysis.features import enrich


FIXTURE_PATH = Path("tests/fixtures/asteroid_data_sample.csv")


def _load_processed():
    df = pd.read_csv(FIXTURE_PATH)
    objects, approaches = build.process_dataframe(df)
    return objects, approaches


def test_aggregates_bins_and_types():
    objects, approaches = _load_processed()
    aggregates = build.compute_aggregates(approaches, objects)
    assert {"monthly_counts", "hazard_rate_size", "top_n"}.issubset(
        set(aggregates["aggregate_type"].dropna().unique())
    )
    hazard_bins = aggregates[aggregates["aggregate_type"] == "hazard_rate_size"]
    assert "size_bin_m" in hazard_bins.columns
    assert hazard_bins["size_bin_m"].notna().any()


def test_hazard_rate_by_size_bin_matches_expected():
    objects, approaches = _load_processed()
    aggregates = build.compute_aggregates(approaches, objects)

    merged = approaches.merge(
        objects[["id", "diameter_mid_m", "diameter_mid_km", "name"]],
        on="id",
        how="left",
    )
    enriched = enrich(merged)
    earth = enriched[enriched["orbiting_body"] == "Earth"]
    expected = (
        earth.groupby("size_bin_m", dropna=False, observed=False)
        .agg(
            total=("id", "size"),
            hazardous=("is_potentially_hazardous_asteroid", "sum"),
        )
        .reset_index()
    )
    expected["hazard_rate"] = expected["hazardous"] / expected["total"]

    actual = aggregates[aggregates["aggregate_type"] == "hazard_rate_size"]
    actual = actual[actual["orbiting_body"] == "Earth"]

    merged_rates = expected.merge(
        actual[["size_bin_m", "hazard_rate"]],
        on="size_bin_m",
        how="left",
        suffixes=("_expected", "_actual"),
    )
    diff = (merged_rates["hazard_rate_expected"] - merged_rates["hazard_rate_actual"]).abs()
    assert diff.fillna(0).max() < 1e-9


def test_top_n_ordering_deterministic():
    objects, approaches = _load_processed()
    aggregates = build.compute_aggregates(approaches, objects)

    top_closest = aggregates[
        (aggregates["aggregate_type"] == "top_n")
        & (aggregates["metric"] == "closest")
        & (aggregates["orbiting_body"] == "Earth")
    ].copy()
    assert not top_closest.empty
    assert top_closest["miss_distance_km"].is_monotonic_increasing

    top_fastest = aggregates[
        (aggregates["aggregate_type"] == "top_n")
        & (aggregates["metric"] == "fastest")
        & (aggregates["orbiting_body"] == "Earth")
    ].copy()
    assert not top_fastest.empty
    assert top_fastest["velocity_km_s"].is_monotonic_decreasing
