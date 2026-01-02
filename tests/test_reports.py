import pandas as pd

from asteroid_analysis.reports import compute_ecdf, compute_monthly_quantiles


def test_monthly_quantiles_shape():
    df = pd.DataFrame(
        {
            "close_approach_date": pd.to_datetime(
                ["2029-01-01", "2029-01-15", "2029-02-01", "2029-02-20"]
            ),
            "miss_distance_km": [10, 100, 20, 200],
            "is_potentially_hazardous_asteroid": [True, True, False, False],
        }
    )
    quantiles = compute_monthly_quantiles(df)
    assert set(quantiles.columns) == {
        "month",
        "is_potentially_hazardous_asteroid",
        "q10",
        "q50",
        "q90",
    }
    assert len(quantiles) == 2


def test_ecdf_monotonic():
    series = pd.Series([3, 1, 2, 2, 5])
    ecdf = compute_ecdf(series)
    assert ecdf["x"].is_monotonic_increasing
    assert ecdf["y"].is_monotonic_increasing
