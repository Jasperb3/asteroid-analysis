import pandas as pd

from asteroid_analysis.features import enrich


def test_bin_edges_and_labels():
    df = pd.DataFrame(
        {
            "miss_distance_lunar": [0.5, 1, 5, 20, 50, 100],
            "diameter_mid_m": [10, 50, 140, 500, 1000, 2000],
            "velocity_km_s": [5, 10, 20, 30, 40, 50],
            "miss_distance_km": [100, 100, 100, 100, 100, 100],
        }
    )
    enriched = enrich(df)

    assert enriched["miss_ld_bin"].tolist() == ["<1", "1-5", "5-20", "20-50", ">50", ">50"]
    assert enriched["size_bin_m"].tolist() == [
        "<50m",
        "50-140m",
        "140-500m",
        "500m-1km",
        ">1km",
        ">1km",
    ]
    assert enriched["velocity_bin_kms"].tolist() == ["<10", "10-20", "20-30", ">30", ">30", ">30"]
    assert str(enriched["size_bin_m"].dtype) == "category"
    assert str(enriched["miss_ld_bin"].dtype) == "category"
    assert str(enriched["velocity_bin_kms"].dtype) == "category"


def test_energy_proxy_non_negative():
    df = pd.DataFrame(
        {
            "miss_distance_lunar": [1],
            "diameter_mid_m": [100],
            "velocity_km_s": [12],
            "miss_distance_km": [1000],
        }
    )
    enriched = enrich(df)
    assert enriched["energy_proxy"].iloc[0] >= 0
