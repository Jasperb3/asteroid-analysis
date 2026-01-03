from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go

from asteroid_analysis.features import enrich


DEFAULT_DATA_DIR = Path("data/processed")


def load_joined(data_dir: Path) -> pd.DataFrame:
    objects_path = data_dir / "objects.parquet"
    approaches_path = data_dir / "approaches.parquet"
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

    # Only merge columns from objects that are not already in approaches
    # Both dataframes have 'id', 'is_potentially_hazardous_asteroid', 'is_sentry_object'
    # So we only need to merge 'diameter_mid_km' and 'diameter_mid_m' from objects
    merged = approaches.merge(
        objects[
            [
                "id",
                "diameter_mid_km",
                "diameter_mid_m",
            ]
        ],
        on="id",
        how="left",
    )
    return merged


def compute_monthly_quantiles(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month"] = df["close_approach_date"].dt.to_period("M").dt.to_timestamp()

    grouped = (
        df.groupby(["month", "is_potentially_hazardous_asteroid"])["miss_distance_km"]
        .quantile([0.1, 0.5, 0.9])
        .unstack(level=-1)
        .reset_index()
    )
    grouped.columns = [
        "month",
        "is_potentially_hazardous_asteroid",
        "q10",
        "q50",
        "q90",
    ]
    return grouped.sort_values("month")


def compute_ecdf(series: pd.Series) -> pd.DataFrame:
    values = series.dropna().sort_values().reset_index(drop=True)
    if values.empty:
        return pd.DataFrame({"x": [], "y": []})
    y = (values.index + 1) / len(values)
    return pd.DataFrame({"x": values, "y": y})


def plot_quantiles_png(
    quantiles: pd.DataFrame, output_path: Path, last_date: pd.Timestamp | None
) -> None:
    plt.figure(figsize=(11, 6))

    for label, group in quantiles.groupby("is_potentially_hazardous_asteroid"):
        name = "Hazardous" if label else "Non-hazardous"
        plt.plot(group["month"], group["q50"], label=f"{name} median")
        plt.fill_between(
            group["month"],
            group["q10"],
            group["q90"],
            alpha=0.2,
            label=f"{name} 10-90%",
        )

    plt.yscale("log")
    plt.title("Monthly Miss Distance Quantiles")
    plt.xlabel("Month")
    plt.ylabel("Miss distance (km, log scale)")
    if last_date is not None and pd.notna(last_date):
        last_month = last_date.to_period("M").to_timestamp()
        month_end = (last_month + pd.offsets.MonthEnd(0)).date()
        if last_date.date() < month_end:
            plt.axvline(last_month, linestyle="--", color="orange")
            plt.text(
                last_month,
                plt.ylim()[1],
                "Last month incomplete",
                color="orange",
                ha="left",
                va="top",
            )
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()


def plot_quantiles_html(
    quantiles: pd.DataFrame, output_path: Path, last_date: pd.Timestamp | None
) -> None:
    fig = go.Figure()
    for label, group in quantiles.groupby("is_potentially_hazardous_asteroid"):
        name = "Hazardous" if label else "Non-hazardous"
        fig.add_trace(
            go.Scatter(
                x=group["month"],
                y=group["q50"],
                mode="lines",
                name=f"{name} median",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=group["month"].tolist() + group["month"][::-1].tolist(),
                y=group["q90"].tolist() + group["q10"][::-1].tolist(),
                fill="toself",
                line=dict(width=0),
                name=f"{name} 10-90%",
                opacity=0.2,
            )
        )

    fig.update_yaxes(type="log", title="Miss distance (km, log scale)")
    fig.update_layout(
        title="Monthly Miss Distance Quantiles",
        xaxis_title="Month",
        height=450,
        margin=dict(l=40, r=20, t=50, b=40),
    )
    if last_date is not None and pd.notna(last_date):
        last_month = last_date.to_period("M").to_timestamp()
        month_end = (last_month + pd.offsets.MonthEnd(0)).date()
        if last_date.date() < month_end:
            fig.add_vline(x=last_month, line_dash="dash", line_color="orange")
            fig.add_annotation(
                x=last_month,
                y=max(quantiles["q90"].dropna()) if not quantiles.empty else 1,
                text="Last month incomplete",
                showarrow=True,
                arrowhead=2,
            )
    fig.write_html(output_path)


def plot_ecdf_png(hazard: pd.Series, non_hazard: pd.Series, output_path: Path) -> None:
    hazard_ecdf = compute_ecdf(hazard)
    non_hazard_ecdf = compute_ecdf(non_hazard)

    plt.figure(figsize=(10, 6))
    plt.plot(hazard_ecdf["x"], hazard_ecdf["y"], label="Hazardous")
    plt.plot(non_hazard_ecdf["x"], non_hazard_ecdf["y"], label="Non-hazardous")
    plt.xscale("log")
    plt.title("ECDF of Miss Distance")
    plt.xlabel("Miss distance (km, log scale)")
    plt.ylabel("ECDF")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()


def plot_ecdf_html(hazard: pd.Series, non_hazard: pd.Series, output_path: Path) -> None:
    hazard_ecdf = compute_ecdf(hazard)
    non_hazard_ecdf = compute_ecdf(non_hazard)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=hazard_ecdf["x"], y=hazard_ecdf["y"], mode="lines", name="Hazardous")
    )
    fig.add_trace(
        go.Scatter(
            x=non_hazard_ecdf["x"], y=non_hazard_ecdf["y"], mode="lines", name="Non-hazardous"
        )
    )
    fig.update_xaxes(type="log", title="Miss distance (km, log scale)")
    fig.update_layout(title="ECDF of Miss Distance", height=450, margin=dict(l=40, r=20, t=50, b=40))
    fig.write_html(output_path)


def plot_weekly_heatmap_html(df: pd.DataFrame, output_path: Path) -> None:
    iso = df["close_approach_date"].dt.isocalendar()
    weekly_counts = (
        df.assign(year=iso.year, week=iso.week)
        .groupby(["year", "week"])
        .size()
        .reset_index(name="count")
    )

    pivot = weekly_counts.pivot(index="year", columns="week", values="count").fillna(0)
    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index.astype(str),
            colorscale="Blues",
            colorbar_title="Approaches",
        )
    )
    fig.update_layout(
        title="Approach Counts by ISO Week",
        xaxis_title="ISO week",
        yaxis_title="Year",
        height=450,
        margin=dict(l=40, r=20, t=50, b=40),
    )
    fig.write_html(output_path)


def build_reports(outdir: Path, orbiting_body: str, data_dir: Path = DEFAULT_DATA_DIR) -> None:
    df = load_joined(data_dir)
    df = df[df["orbiting_body"] == orbiting_body]
    df = df.dropna(subset=["close_approach_date", "miss_distance_km"])
    df = enrich(df)
    if df.empty:
        print(
            "No data available after filtering. "
            "Check orbiting body or run build to regenerate processed tables."
        )
        return

    outdir.mkdir(parents=True, exist_ok=True)

    quantiles = compute_monthly_quantiles(df)
    last_date = df["close_approach_date"].max()
    plot_quantiles_png(quantiles, outdir / "miss_distance_quantiles.png", last_date)
    plot_quantiles_html(quantiles, outdir / "miss_distance_quantiles.html", last_date)

    hazard = df[df["is_potentially_hazardous_asteroid"]]["miss_distance_km"]
    non_hazard = df[~df["is_potentially_hazardous_asteroid"]]["miss_distance_km"]
    plot_ecdf_png(hazard, non_hazard, outdir / "miss_distance_ecdf.png")
    plot_ecdf_html(hazard, non_hazard, outdir / "miss_distance_ecdf.html")

    plot_weekly_heatmap_html(df, outdir / "approaches_calendar_heatmap.html")


def main():
    parser = argparse.ArgumentParser(description="Generate asteroid approach reports.")
    parser.add_argument(
        "--outdir",
        default="outputs/reports",
        help="Output directory for report files.",
    )
    parser.add_argument(
        "--orbiting-body",
        default="Earth",
        help="Orbiting body to filter approaches by.",
    )
    parser.add_argument(
        "--data-dir",
        default="data/processed",
        help="Directory containing processed parquet tables.",
    )
    args = parser.parse_args()

    build_reports(Path(args.outdir), args.orbiting_body, Path(args.data_dir))


if __name__ == "__main__":
    main()
