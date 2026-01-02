import argparse
import math
from pathlib import Path

import pandas as pd

from asteroid_analysis.metadata import build_metadata, write_metadata
REQUIRED_COLUMNS = [
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

OBJECT_COLUMNS = [
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
    "diameter_mid_km",
    "diameter_mid_m",
    "diameter_uncertainty_ratio_km",
    "log_diameter_mid_km",
]

APPROACH_COLUMNS = [
    "approach_id",
    "id",
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
    "log_miss_distance_km",
]


def _safe_log10(series: pd.Series) -> pd.Series:
    def to_log(value):
        if pd.isna(value) or value <= 0:
            return pd.NA
        return math.log10(value)

    return series.apply(to_log)


def _first_non_null(series: pd.Series):
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    return non_null.iloc[0]


def process_dataframe(df: pd.DataFrame):
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

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

    df["close_approach_date"] = pd.to_datetime(df["close_approach_date"], errors="coerce")
    df["close_approach_date_full"] = pd.to_datetime(
        df["close_approach_date_full"], errors="coerce"
    )

    for col in ["is_potentially_hazardous_asteroid", "is_sentry_object"]:
        df[col] = df[col].astype("boolean").fillna(False).astype(bool)

    df["diameter_mid_km"] = (df["diameter_km_min"] + df["diameter_km_max"]) / 2
    df["diameter_mid_m"] = (df["diameter_m_min"] + df["diameter_m_max"]) / 2
    df["diameter_uncertainty_ratio_km"] = (
        (df["diameter_km_max"] - df["diameter_km_min"]) / df["diameter_mid_km"]
    )
    df.loc[df["diameter_mid_km"] == 0, "diameter_uncertainty_ratio_km"] = pd.NA

    epoch_str = df["epoch_date_close_approach"].apply(
        lambda value: str(int(value)) if pd.notna(value) else "nan"
    )
    df["approach_id"] = df["id"].astype(str) + "_" + epoch_str

    # Remove log columns from the columns to be grouped
    object_fields = [col for col in OBJECT_COLUMNS if col != "id" and col != "log_diameter_mid_km"]
    objects = (
        df[["id"] + object_fields]
        .groupby("id", dropna=False)[object_fields]
        .agg(_first_non_null)
        .reset_index()
    )

    # Calculate log values after grouping to ensure they're derived from the same row values
    objects["log_diameter_mid_km"] = _safe_log10(objects["diameter_mid_km"])

    # For approaches, we need to exclude the log column from initial selection
    approach_fields = [col for col in APPROACH_COLUMNS if col != "log_miss_distance_km"]
    approaches = df[approach_fields].copy()
    approaches["log_miss_distance_km"] = _safe_log10(approaches["miss_distance_km"])
    approaches["is_potentially_hazardous_asteroid"] = df[
        "is_potentially_hazardous_asteroid"
    ]
    approaches["is_sentry_object"] = df["is_sentry_object"]
    approaches = approaches.drop_duplicates("approach_id", keep="first")

    return objects, approaches


def build_tables(input_path: Path, outdir: Path):
    df = pd.read_csv(input_path)
    objects, approaches = process_dataframe(df)

    outdir.mkdir(parents=True, exist_ok=True)
    objects_path = outdir / "objects"
    approaches_path = outdir / "approaches"

    objects.to_parquet(f"{objects_path}.parquet", index=False)
    objects.to_csv(f"{objects_path}.csv", index=False)
    approaches.to_parquet(f"{approaches_path}.parquet", index=False)
    approaches.to_csv(f"{approaches_path}.csv", index=False)

    metadata = build_metadata(
        df=approaches,
        input_path=input_path,
        orbiting_body_filter="all",
    )
    metadata_path = outdir / "metadata.json"
    write_metadata(metadata, metadata_path)

    print(f"Total approaches: {metadata.total_approaches}")
    print(f"Unique objects: {metadata.unique_objects}")
    print(f"Hazardous objects: {metadata.hazardous_objects}")
    print(f"Sentry objects: {metadata.sentry_objects}")


def main():
    parser = argparse.ArgumentParser(description="Build asteroid analysis tables.")
    parser.add_argument(
        "--input",
        default="asteroid_data_full.csv",
        help="Path to the input CSV file.",
    )
    parser.add_argument(
        "--outdir",
        default="data/processed",
        help="Directory to write processed tables.",
    )
    args = parser.parse_args()

    build_tables(Path(args.input), Path(args.outdir))


if __name__ == "__main__":
    main()
