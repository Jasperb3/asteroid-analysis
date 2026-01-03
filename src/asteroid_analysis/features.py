import pandas as pd


MISS_LD_BINS = [-float("inf"), 1, 5, 20, 50, float("inf")]
MISS_LD_LABELS = ["<1", "1-5", "5-20", "20-50", ">50"]

SIZE_BINS_M = [-float("inf"), 50, 140, 500, 1000, float("inf")]
SIZE_LABELS_M = ["<50m", "50-140m", "140-500m", "500m-1km", ">1km"]

VELOCITY_BINS_KMS = [-float("inf"), 10, 20, 30, float("inf")]
VELOCITY_LABELS_KMS = ["<10", "10-20", "20-30", ">30"]


def _normalize_rank(series: pd.Series, ascending: bool) -> pd.Series:
    ranks = series.rank(method="average", ascending=ascending, na_option="keep")
    min_rank = ranks.min()
    max_rank = ranks.max()
    if pd.isna(min_rank) or pd.isna(max_rank) or max_rank == min_rank:
        return ranks * 0
    return (ranks - min_rank) / (max_rank - min_rank)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["miss_distance_ld"] = df["miss_distance_lunar"]
    df["miss_ld_bin"] = pd.cut(
        df["miss_distance_ld"],
        bins=MISS_LD_BINS,
        labels=MISS_LD_LABELS,
        right=False,
    )
    df["miss_ld_bin"] = df["miss_ld_bin"].astype("category")
    df["size_bin_m"] = pd.cut(
        df["diameter_mid_m"],
        bins=SIZE_BINS_M,
        labels=SIZE_LABELS_M,
        right=False,
    )
    df["size_bin_m"] = df["size_bin_m"].astype("category")
    df["velocity_bin_kms"] = pd.cut(
        df["velocity_km_s"],
        bins=VELOCITY_BINS_KMS,
        labels=VELOCITY_LABELS_KMS,
        right=False,
    )
    df["velocity_bin_kms"] = df["velocity_bin_kms"].astype("category")

    df["energy_proxy"] = (df["diameter_mid_m"] ** 3) * (
        (df["velocity_km_s"] * 1000) ** 2
    )

    rank_close = 1 - _normalize_rank(df["miss_distance_km"], ascending=True)
    rank_size = _normalize_rank(df["diameter_mid_m"], ascending=True)
    rank_speed = _normalize_rank(df["velocity_km_s"], ascending=True)

    df["rank_close"] = rank_close
    df["rank_size"] = rank_size
    df["rank_speed"] = rank_speed

    df["interesting_score"] = 0.5 * rank_close + 0.3 * rank_size + 0.2 * rank_speed

    if "orbiting_body" in df.columns:
        df["orbiting_body"] = df["orbiting_body"].astype("category")
    if "orbit_class_name" in df.columns:
        df["orbit_class_name"] = df["orbit_class_name"].astype("category")

    return df
