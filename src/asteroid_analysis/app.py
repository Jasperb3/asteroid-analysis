from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from asteroid_analysis.reports import build_reports
from asteroid_analysis.features import enrich
from asteroid_analysis.metadata import build_metadata, write_metadata

DATA_DIR = Path("data/processed")

REQUIRED_OBJECT_COLUMNS = [
    "id",
    "name",
    "nasa_jpl_url",
    "absolute_magnitude_h",
    "is_potentially_hazardous_asteroid",
    "is_sentry_object",
    "diameter_km_min",
    "diameter_km_max",
    "diameter_mid_km",
    "diameter_m_min",
    "diameter_m_max",
    "diameter_mid_m",
]

REQUIRED_APPROACH_COLUMNS = [
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
]


def get_missing_processed_paths(data_dir: Path):
    objects_path = data_dir / "objects.parquet"
    approaches_path = data_dir / "approaches.parquet"
    missing = [path for path in [objects_path, approaches_path] if not path.exists()]
    return missing


def get_data_mtimes(data_dir: Path):
    objects_path = data_dir / "objects.parquet"
    approaches_path = data_dir / "approaches.parquet"
    orbits_path = data_dir / "orbits.parquet"
    aggregates_path = data_dir / "aggregates.parquet"
    return (
        objects_path.stat().st_mtime if objects_path.exists() else 0,
        approaches_path.stat().st_mtime if approaches_path.exists() else 0,
        orbits_path.stat().st_mtime if orbits_path.exists() else 0,
        aggregates_path.stat().st_mtime if aggregates_path.exists() else 0,
    )


def load_dataframes(data_dir: Path):
    objects_path = data_dir / "objects.parquet"
    approaches_path = data_dir / "approaches.parquet"
    orbits_path = data_dir / "orbits.parquet"
    aggregates_path = data_dir / "aggregates.parquet"

    objects = pd.read_parquet(objects_path)
    approaches = pd.read_parquet(approaches_path)
    merged = approaches.merge(
        objects[REQUIRED_OBJECT_COLUMNS],
        on="id",
        how="left",
    )

    orbits = None
    if orbits_path.exists():
        orbits = pd.read_parquet(orbits_path)
        merged = merged.merge(orbits, on="id", how="left")

    aggregates = None
    if aggregates_path.exists():
        aggregates = pd.read_parquet(aggregates_path)

    return objects, approaches, merged, orbits, aggregates


@st.cache_data(show_spinner="Loading data...")
def load_data(data_dir: Path, mtimes):
    return load_dataframes(data_dir)


def build_monthly_heatmap(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title="Monthly Approach Volume",
            height=350,
            margin=dict(l=40, r=20, t=50, b=40),
        )
        return fig

    monthly = (
        df.set_index("close_approach_date")
        .sort_index()
        .resample("M")
        .size()
        .rename("count")
        .reset_index()
    )
    monthly["year"] = monthly["close_approach_date"].dt.year
    monthly["month"] = monthly["close_approach_date"].dt.month

    heatmap_data = (
        monthly.pivot(index="year", columns="month", values="count")
        .fillna(0)
        .sort_index()
    )

    fig = go.Figure(
        data=go.Heatmap(
            z=heatmap_data.values,
            x=[date(2000, m, 1).strftime("%b") for m in heatmap_data.columns],
            y=heatmap_data.index.astype(str),
            colorscale="Blues",
            colorbar_title="Approaches",
        )
    )
    fig.update_layout(
        title="Monthly Approach Volume",
        xaxis_title="Month",
        yaxis_title="Year",
        height=350,
        margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


def main():
    st.set_page_config(page_title="Asteroid Approaches Explorer", layout="wide")
    st.title("Asteroid Close Approaches Explorer")

    missing = get_missing_processed_paths(DATA_DIR)
    if missing:
        st.error(
            "Missing processed tables: "
            + ", ".join(str(path) for path in missing)
            + "\nRun: python -m asteroid_analysis.build --input asteroid_data_full.csv --outdir data/processed"
        )
        st.stop()

    mtimes = get_data_mtimes(DATA_DIR)
    objects, approaches, merged, orbits, aggregates = load_data(DATA_DIR, mtimes)

    if merged.empty:
        st.warning("Processed tables are empty. Run ingestion/build to populate data.")
        st.stop()

    st.sidebar.header("Filters")
    st.sidebar.header("Maintenance")
    if st.sidebar.button("Clear app cache"):
        st.cache_data.clear()
        st.rerun()

    min_date = merged["close_approach_date"].min()
    max_date = merged["close_approach_date"].max()
    if pd.isna(min_date) or pd.isna(max_date):
        st.error("Missing close_approach_date values in processed data.")
        st.stop()
    date_range = st.sidebar.date_input(
        "Close approach date range",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    hazard_filter = st.sidebar.selectbox(
        "Hazard status",
        ["All", "Hazardous only", "Non-hazardous only"],
    )
    sentry_only = st.sidebar.checkbox("Sentry objects only", value=False)

    miss_values = merged["miss_distance_km"].dropna()
    miss_disabled = miss_values.empty
    miss_min = float(miss_values.min()) if not miss_disabled else 0.0
    miss_max = float(miss_values.max()) if not miss_disabled else 0.0
    miss_range = st.sidebar.slider(
        "Miss distance (km)",
        min_value=miss_min,
        max_value=miss_max,
        value=(miss_min, miss_max),
        disabled=miss_disabled,
    )

    vel_values = merged["velocity_km_s"].dropna()
    vel_disabled = vel_values.empty
    vel_min = float(vel_values.min()) if not vel_disabled else 0.0
    vel_max = float(vel_values.max()) if not vel_disabled else 0.0
    vel_range = st.sidebar.slider(
        "Velocity (km/s)",
        min_value=vel_min,
        max_value=vel_max,
        value=(vel_min, vel_max),
        disabled=vel_disabled,
    )

    diam_values = merged["diameter_mid_km"].dropna()
    diam_disabled = diam_values.empty
    diam_min = float(diam_values.min()) if not diam_disabled else 0.0
    diam_max = float(diam_values.max()) if not diam_disabled else 0.0
    diam_range = st.sidebar.slider(
        "Diameter mid (km)",
        min_value=diam_min,
        max_value=diam_max,
        value=(diam_min, diam_max),
        disabled=diam_disabled,
    )

    orbiting_options = sorted(merged["orbiting_body"].dropna().unique().tolist())
    if not orbiting_options:
        st.error("No orbiting body values available in processed data.")
        st.stop()
    default_orbit = orbiting_options.index("Earth") if "Earth" in orbiting_options else 0
    orbiting_body = st.sidebar.selectbox(
        "Orbiting body",
        orbiting_options,
        index=default_orbit,
    )

    orbit_class_filter = None
    moid_range = None
    moid_min = None
    moid_max = None
    if orbits is not None and "orbit_class_name" in merged.columns:
        classes = sorted(merged["orbit_class_name"].dropna().unique().tolist())
        orbit_class_filter = st.sidebar.selectbox(
            "Orbit class",
            ["All"] + classes,
        )
        if merged["minimum_orbit_intersection"].notna().any():
            moid_min = float(merged["minimum_orbit_intersection"].min())
            moid_max = float(merged["minimum_orbit_intersection"].max())
            moid_range = st.sidebar.slider(
                "Minimum orbit intersection (AU)",
                min_value=moid_min,
                max_value=moid_max,
                value=(moid_min, moid_max),
            )

    filtered = merged.copy()
    if isinstance(date_range, tuple):
        start_date, end_date = date_range
        filtered = filtered[
            (filtered["close_approach_date"].dt.date >= start_date)
            & (filtered["close_approach_date"].dt.date <= end_date)
        ]

    if hazard_filter == "Hazardous only":
        filtered = filtered[filtered["is_potentially_hazardous_asteroid"]]
    elif hazard_filter == "Non-hazardous only":
        filtered = filtered[~filtered["is_potentially_hazardous_asteroid"]]

    if sentry_only:
        filtered = filtered[filtered["is_sentry_object"]]

    filtered = filtered[
        (filtered["miss_distance_km"] >= miss_range[0])
        & (filtered["miss_distance_km"] <= miss_range[1])
    ]
    filtered = filtered[
        (filtered["velocity_km_s"] >= vel_range[0])
        & (filtered["velocity_km_s"] <= vel_range[1])
    ]
    filtered = filtered[
        (filtered["diameter_mid_km"] >= diam_range[0])
        & (filtered["diameter_mid_km"] <= diam_range[1])
    ]

    filtered = filtered[filtered["orbiting_body"] == orbiting_body]

    if orbit_class_filter and orbit_class_filter != "All":
        filtered = filtered[filtered["orbit_class_name"] == orbit_class_filter]
    if moid_range is not None:
        filtered = filtered[
            (filtered["minimum_orbit_intersection"] >= moid_range[0])
            & (filtered["minimum_orbit_intersection"] <= moid_range[1])
        ]

    filtered = enrich(filtered)

    if filtered.empty:
        st.warning("No data matches the current filters.")
        st.stop()

    pre_counts = {
        "total_approaches": len(merged),
        "unique_objects": merged["id"].nunique(),
        "hazardous_objects": objects["is_potentially_hazardous_asteroid"].sum(),
        "hazardous_approaches": merged["is_potentially_hazardous_asteroid"].sum(),
        "sentry_objects": objects["is_sentry_object"].sum(),
        "sentry_approaches": merged["is_sentry_object"].sum(),
    }

    filtered_objects = filtered.drop_duplicates("id")
    post_counts = {
        "total_approaches": len(filtered),
        "unique_objects": filtered["id"].nunique(),
        "hazardous_objects": filtered_objects["is_potentially_hazardous_asteroid"].sum(),
        "hazardous_approaches": filtered["is_potentially_hazardous_asteroid"].sum(),
        "sentry_objects": filtered_objects["is_sentry_object"].sum(),
        "sentry_approaches": filtered["is_sentry_object"].sum(),
    }

    tab_overview, tab_size, tab_rank, tab_apophis, tab_reports = st.tabs(
        ["Overview", "Size vs Distance", "Rankings", "Apophis", "Reports"]
    )

    aggregates_ready = (
        aggregates is not None
        and "aggregate_type" in aggregates.columns
        and "orbiting_body" in aggregates.columns
    )
    default_filters = (
        date_range == (min_date.date(), max_date.date())
        and hazard_filter == "All"
        and not sentry_only
        and (miss_range == (miss_min, miss_max) if not miss_disabled else True)
        and (vel_range == (vel_min, vel_max) if not vel_disabled else True)
        and (diam_range == (diam_min, diam_max) if not diam_disabled else True)
        and (orbit_class_filter in (None, "All"))
        and (moid_range is None or moid_range == (moid_min, moid_max))
    )
    use_aggregates = aggregates_ready and default_filters

    with tab_overview:
        st.subheader("Key metrics (filtered vs all)")
        metrics = [
            ("Total approaches", "total_approaches"),
            ("Unique objects", "unique_objects"),
            ("Hazardous objects", "hazardous_objects"),
            ("Hazardous approaches", "hazardous_approaches"),
            ("Sentry objects", "sentry_objects"),
            ("Sentry approaches", "sentry_approaches"),
        ]
        cols = st.columns(3)
        for idx, (label, key) in enumerate(metrics):
            delta = post_counts[key] - pre_counts[key]
            cols[idx % 3].metric(label, post_counts[key], delta=delta)

        st.subheader("Approaches per month")
        if use_aggregates:
            monthly = aggregates[aggregates["aggregate_type"] == "monthly_counts"]
            monthly = monthly[monthly["orbiting_body"] == orbiting_body]
            monthly = (
                monthly.groupby("month", dropna=False)["count"]
                .sum()
                .reset_index()
                .rename(columns={"month": "close_approach_date"})
            )
        else:
            monthly = (
                filtered.set_index("close_approach_date")
                .sort_index()
                .resample("M")
                .size()
                .rename("count")
                .reset_index()
            )
        line_fig = px.line(monthly, x="close_approach_date", y="count")
        line_fig.update_layout(height=300, margin=dict(l=40, r=20, t=30, b=40))

        if not monthly.empty:
            last_month = monthly["close_approach_date"].max()
            last_date = filtered["close_approach_date"].max()
            if last_date is not pd.NaT and last_date.day < last_month.days_in_month:
                line_fig.add_vline(
                    x=last_month,
                    line_dash="dash",
                    line_color="orange",
                )
                line_fig.add_annotation(
                    x=last_month,
                    y=monthly.loc[monthly["close_approach_date"] == last_month, "count"].iloc[0],
                    text="Last month incomplete",
                    showarrow=True,
                    arrowhead=2,
                )
                st.warning("Latest month is incomplete; interpret trends with caution.")
        st.plotly_chart(line_fig, use_container_width=True)

        st.subheader("Monthly heatmap")
        st.plotly_chart(build_monthly_heatmap(filtered), use_container_width=True)

        st.subheader("Hazard rate by size bin")
        if use_aggregates:
            size_counts = aggregates[aggregates["aggregate_type"] == "hazard_rate_size"]
            size_counts = size_counts[size_counts["orbiting_body"] == orbiting_body]
        else:
            size_counts = (
                filtered.groupby("size_bin_m", dropna=False)
                .agg(
                    total=("id", "size"),
                    hazardous=("is_potentially_hazardous_asteroid", "sum"),
                )
                .reset_index()
            )
            size_counts["hazard_rate"] = size_counts["hazardous"] / size_counts["total"]
        bar_fig = px.bar(
            size_counts,
            x="size_bin_m",
            y="hazard_rate",
            text="total",
            labels={"hazard_rate": "Hazard rate", "size_bin_m": "Size bin"},
        )
        bar_fig.update_layout(height=350, margin=dict(l=40, r=20, t=30, b=40))
        st.plotly_chart(bar_fig, use_container_width=True)

        if orbits is not None and "orbit_class_name" in filtered.columns:
            st.subheader("Orbit class mix")
            orbit_counts = (
                filtered.groupby("orbit_class_name", dropna=False)
                .size()
                .reset_index(name="count")
            )
            orbit_fig = px.bar(
                orbit_counts,
                x="orbit_class_name",
                y="count",
                labels={"orbit_class_name": "Orbit class"},
            )
            orbit_fig.update_layout(height=350, margin=dict(l=40, r=20, t=30, b=40))
            st.plotly_chart(orbit_fig, use_container_width=True)

    with tab_size:
        st.subheader("Size vs Distance")
        show_density = st.checkbox("Show density instead of points", value=False)
        top_n = st.number_input("Top N labeled points", min_value=5, max_value=100, value=20)

        plot_df = filtered.copy()
        plot_df = plot_df[
            (plot_df["miss_distance_km"] > 0) & (plot_df["diameter_mid_km"] > 0)
        ]

        if show_density:
            fig = px.density_heatmap(
                plot_df,
                x="miss_distance_km",
                y="diameter_mid_km",
                nbinsx=40,
                nbinsy=40,
                color_continuous_scale="Viridis",
            )
        else:
            fig = px.scatter(
                plot_df,
                x="miss_distance_km",
                y="diameter_mid_km",
                color="is_potentially_hazardous_asteroid",
                symbol="is_sentry_object",
                hover_data={
                    "name": True,
                    "id": True,
                    "close_approach_date": True,
                    "miss_distance_km": True,
                    "miss_distance_lunar": True,
                    "velocity_km_s": True,
                    "diameter_km_min": True,
                    "diameter_km_max": True,
                    "absolute_magnitude_h": True,
                    "nasa_jpl_url": True,
                },
            )

        fig.update_xaxes(type="log", title="Miss distance (km)")
        fig.update_yaxes(type="log", title="Diameter mid (km)")
        fig.update_layout(height=500, margin=dict(l=40, r=20, t=30, b=40))

        closest = plot_df.nsmallest(top_n, "miss_distance_km")
        largest = plot_df.nlargest(top_n, "diameter_mid_km")
        for label_df, label in [(closest, "Closest"), (largest, "Largest")]:
            fig.add_trace(
                go.Scatter(
                    x=label_df["miss_distance_km"],
                    y=label_df["diameter_mid_km"],
                    mode="markers+text",
                    text=label_df["name"],
                    textposition="top center",
                    marker=dict(size=8, color="black", symbol="circle-open"),
                    name=f"{label} {top_n}",
                    showlegend=True,
                )
            )

        st.plotly_chart(fig, use_container_width=True)

    with tab_rank:
        st.subheader("Rankings")
        ranking_metric = st.selectbox(
            "Metric",
            [
                "closest miss_distance_km",
                "largest diameter_mid_km",
                "fastest velocity_km_s",
                "highest energy_proxy",
            ],
        )

        ranked = filtered.copy()

        if ranking_metric == "closest miss_distance_km":
            ranked = ranked.nsmallest(50, "miss_distance_km")
        elif ranking_metric == "largest diameter_mid_km":
            ranked = ranked.nlargest(50, "diameter_mid_km")
        elif ranking_metric == "fastest velocity_km_s":
            ranked = ranked.nlargest(50, "velocity_km_s")
        else:
            ranked = ranked.nlargest(50, "energy_proxy")

        ranked = ranked.copy()
        ranked["nasa_jpl_url"] = ranked["nasa_jpl_url"].apply(
            lambda url: f"[link]({url})" if pd.notna(url) else ""
        )

        table_md = (
            ranked[
                [
                    "name",
                    "id",
                    "close_approach_date",
                    "miss_distance_km",
                    "velocity_km_s",
                    "diameter_mid_km",
                    "energy_proxy",
                    "nasa_jpl_url",
                ]
            ]
            .rename(columns={"nasa_jpl_url": "nasa_jpl_url (link)"})
            .to_markdown(index=False)
        )
        st.markdown(table_md)
        st.caption("Energy proxy = diameter_mid_m^3 * (velocity_km_s * 1000)^2")

    with tab_apophis:
        st.subheader("Apophis close approaches")
        apophis = filtered[
            filtered["name"].str.contains("Apophis", case=False, na=False)
            | filtered["id"].astype(str).isin({"2099942", "99942"})
        ]
        if apophis.empty:
            st.info("No Apophis records in the current filter selection.")
        else:
            apophis = apophis.sort_values("close_approach_date")
            fig = px.line(
                apophis,
                x="close_approach_date",
                y="miss_distance_km",
                markers=True,
            )
            fig.update_yaxes(type="log", title="Miss distance (km)")
            fig.update_layout(height=400, margin=dict(l=40, r=20, t=30, b=40))

            closest = apophis.loc[apophis["miss_distance_km"].idxmin()]
            fig.add_annotation(
                x=closest["close_approach_date"],
                y=closest["miss_distance_km"],
                text=f"Closest: {closest['miss_distance_km']:.0f} km",
                showarrow=True,
                arrowhead=2,
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab_reports:
        st.subheader("Generate static reports")
        st.write(
            "Create static PNG/HTML reports for miss-distance quantiles, ECDFs, and weekly heatmaps."
        )
        report_outdir = st.text_input("Output directory", value="outputs/reports")
        report_body = st.text_input("Orbiting body", value=orbiting_body)
        if st.button("Generate reports"):
            build_reports(Path(report_outdir), report_body, DATA_DIR)
            st.success(f"Reports written to {report_outdir}")

    metadata = build_metadata(
        df=filtered,
        input_path=Path("data/processed/approaches.parquet"),
        orbiting_body_filter=orbiting_body,
    )
    write_metadata(metadata, Path("outputs/metadata.json"))


if __name__ == "__main__":
    main()
