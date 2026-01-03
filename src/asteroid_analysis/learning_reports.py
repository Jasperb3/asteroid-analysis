import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px

from asteroid_analysis.features import enrich


INTERPRETATION_NOTES = """# Interpretation Notes

- The PHA (potentially hazardous asteroid) flag is not an impact prediction.
- The energy_proxy metric is a heuristic signal; it is not a probability.
- Rows represent close-approach events, not trajectories.
- MOID (minimum orbit intersection distance) is a long-term orbital metric,
  while close-approach distance is a specific event distance for a given date.
- Recent months may be partial depending on the run window.
"""


def load_processed(data_dir: Path) -> pd.DataFrame:
    objects_path = data_dir / "objects.parquet"
    approaches_path = data_dir / "approaches.parquet"
    orbits_path = data_dir / "orbits.parquet"
    missing = [path for path in [objects_path, approaches_path] if not path.exists()]
    if missing:
        missing_list = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(
            "Missing processed parquet files: "
            f"{missing_list}. Run: python -m asteroid_analysis.build "
            f"--input asteroid_data_full.csv --outdir {data_dir}"
        )

    objects = pd.read_parquet(objects_path)
    approaches = pd.read_parquet(approaches_path)
    merged = approaches.merge(
        objects[
            [
                "id",
                "name",
                "nasa_jpl_url",
                "diameter_mid_m",
            ]
        ],
        on="id",
        how="left",
    )
    if orbits_path.exists():
        orbits = pd.read_parquet(orbits_path)
        # Ensure id column types match before merging
        merged["id"] = merged["id"].astype(str)
        orbits["id"] = orbits["id"].astype(str)
        merged = merged.merge(
            orbits[["id", "orbit_class_name", "minimum_orbit_intersection"]],
            on="id",
            how="left",
        )
    return merged


def build_learning_reports(
    outdir: Path,
    data_dir: Path,
    orbiting_body: str,
    as_of_date: datetime | None = None,
):
    df = load_processed(data_dir)
    metadata_path = data_dir / "metadata.json"
    duplicate_count = 0
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            duplicate_count = int(metadata.get("duplicate_approach_id_count", 0) or 0)
        except json.JSONDecodeError:
            duplicate_count = 0
    df = df[df["orbiting_body"] == orbiting_body]
    df = df.dropna(subset=["close_approach_date"])

    if df.empty:
        print("No data available for learning reports.")
        return

    df = enrich(df)
    outdir.mkdir(parents=True, exist_ok=True)

    if as_of_date is None:
        as_of_date = datetime.now()
    window_end = as_of_date + timedelta(days=90)
    upcoming = df[
        (df["close_approach_date"] >= as_of_date)
        & (df["close_approach_date"] <= window_end)
    ].sort_values("miss_distance_km")

    watchlist_columns = [
        "name",
        "close_approach_date",
        "miss_distance_lunar",
        "velocity_km_s",
        "diameter_mid_m",
        "is_potentially_hazardous_asteroid",
        "is_sentry_object",
        "nasa_jpl_url",
    ]
    if "orbit_class_name" in upcoming.columns:
        watchlist_columns.append("orbit_class_name")
    if "minimum_orbit_intersection" in upcoming.columns:
        watchlist_columns.append("minimum_orbit_intersection")

    watchlist = upcoming[watchlist_columns].rename(
        columns={
            "close_approach_date": "date",
            "is_potentially_hazardous_asteroid": "hazardous",
            "is_sentry_object": "sentry",
            "nasa_jpl_url": "jpl_url",
            "orbit_class_name": "orbit_class",
            "minimum_orbit_intersection": "moid_au",
        }
    )
    watchlist.to_csv(outdir / "watchlist_next_90_days.csv", index=False)

    near_miss = df[df["miss_distance_lunar"] <= 5]
    timeline_fig = px.scatter(
        near_miss,
        x="close_approach_date",
        y="miss_distance_lunar",
        color="is_potentially_hazardous_asteroid",
        symbol="is_sentry_object",
        hover_data={
            "name": True,
            "id": True,
            "close_approach_date": True,
            "miss_distance_lunar": True,
            "miss_distance_km": True,
            "velocity_km_s": True,
            "diameter_mid_m": True,
            "nasa_jpl_url": True,
        },
    )
    timeline_fig.update_layout(
        title="Near-miss events (<= 5 LD)",
        xaxis_title="Date",
        yaxis_title="Miss distance (lunar distances)",
        height=450,
        margin=dict(l=40, r=20, t=50, b=40),
    )
    if duplicate_count > 0:
        timeline_fig.add_annotation(
            x=0.01,
            y=0.01,
            xref="paper",
            yref="paper",
            text=f"Note: {duplicate_count} duplicate approach_id values detected.",
            showarrow=False,
            align="left",
        )
    timeline_fig.write_html(outdir / "near_misses_under_5LD.html")

    hazard_by_approach = (
        df.groupby("size_bin_m", dropna=False, observed=False)
        .agg(
            total=("id", "size"),
            hazardous=("is_potentially_hazardous_asteroid", "sum"),
        )
        .reset_index()
    )
    hazard_by_approach["rate"] = (
        hazard_by_approach["hazardous"] / hazard_by_approach["total"]
    )
    hazard_by_approach["scope"] = "Approaches"

    objects = df.drop_duplicates("id")
    hazard_by_object = (
        objects.groupby("size_bin_m", dropna=False, observed=False)
        .agg(
            total=("id", "size"),
            hazardous=("is_potentially_hazardous_asteroid", "sum"),
        )
        .reset_index()
    )
    hazard_by_object["rate"] = hazard_by_object["hazardous"] / hazard_by_object["total"]
    hazard_by_object["scope"] = "Unique objects"

    hazard_bins = pd.concat([hazard_by_approach, hazard_by_object], ignore_index=True)
    hazard_fig = px.bar(
        hazard_bins,
        x="size_bin_m",
        y="rate",
        color="scope",
        barmode="group",
        labels={"rate": "Hazard rate", "size_bin_m": "Size bin"},
    )
    hazard_fig.update_layout(
        title="Hazard rate by size bin",
        height=400,
        margin=dict(l=40, r=20, t=50, b=40),
    )
    if duplicate_count > 0:
        hazard_fig.add_annotation(
            x=0.01,
            y=0.01,
            xref="paper",
            yref="paper",
            text=f"Note: {duplicate_count} duplicate approach_id values detected.",
            showarrow=False,
            align="left",
        )
    hazard_fig.write_html(outdir / "hazard_vs_size_bins.html")

    if "minimum_orbit_intersection" in df.columns:
        moid_df = df.dropna(
            subset=["minimum_orbit_intersection", "miss_distance_km"]
        )
        moid_fig = px.scatter(
            moid_df,
            x="minimum_orbit_intersection",
            y="miss_distance_km",
            color="is_potentially_hazardous_asteroid",
            hover_data={
                "name": True,
                "id": True,
                "close_approach_date": True,
                "miss_distance_km": True,
                "minimum_orbit_intersection": True,
                "velocity_km_s": True,
            },
        )
        moid_fig.update_layout(
            title="MOID vs close-approach distance",
            xaxis_title="Minimum orbit intersection (AU)",
            yaxis_title="Miss distance (km)",
            height=450,
            margin=dict(l=40, r=20, t=50, b=40),
        )
        if duplicate_count > 0:
            moid_fig.add_annotation(
                x=0.01,
                y=0.01,
                xref="paper",
                yref="paper",
                text=f"Note: {duplicate_count} duplicate approach_id values detected.",
                showarrow=False,
                align="left",
            )
        moid_fig.write_html(outdir / "moid_vs_miss_distance.html")

    notes_path = outdir / "interpretation_notes.md"
    notes_path.write_text(INTERPRETATION_NOTES)


def main():
    parser = argparse.ArgumentParser(
        description="Generate learning-focused reports for NEO monitoring."
    )
    parser.add_argument(
        "--data-dir",
        default="data/processed",
        help="Directory containing processed parquet tables.",
    )
    parser.add_argument(
        "--outdir",
        default="outputs/learning",
        help="Output directory for learning reports.",
    )
    parser.add_argument(
        "--orbiting-body",
        default="Earth",
        help="Orbiting body filter.",
    )
    args = parser.parse_args()

    build_learning_reports(
        outdir=Path(args.outdir),
        data_dir=Path(args.data_dir),
        orbiting_body=args.orbiting_body,
    )


if __name__ == "__main__":
    main()
