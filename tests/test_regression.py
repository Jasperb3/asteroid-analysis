from pathlib import Path

import pandas as pd
from pandas.api import types as ptypes

from asteroid_analysis import build, reports


FIXTURE_PATH = Path("tests/fixtures/asteroid_data_sample.csv")


def _load_fixture_df():
    return pd.read_csv(FIXTURE_PATH)


def test_build_outputs_schema_and_types():
    df = _load_fixture_df()
    objects, approaches = build.process_dataframe(df)

    expected_objects = set(build.OBJECT_COLUMNS)
    expected_approaches = set(build.APPROACH_COLUMNS) | {
        "is_potentially_hazardous_asteroid",
        "is_sentry_object",
    }

    assert expected_objects.issubset(objects.columns)
    assert expected_approaches.issubset(approaches.columns)

    assert ptypes.is_datetime64_any_dtype(approaches["close_approach_date"])
    assert ptypes.is_datetime64_any_dtype(approaches["close_approach_date_full"])
    assert ptypes.is_bool_dtype(approaches["is_potentially_hazardous_asteroid"])
    assert ptypes.is_bool_dtype(objects["is_sentry_object"])
    assert ptypes.is_numeric_dtype(approaches["miss_distance_km"])


def test_no_negative_distances_or_velocities():
    df = _load_fixture_df()
    _, approaches = build.process_dataframe(df)
    assert approaches["miss_distance_km"].min() >= 0
    assert approaches["velocity_km_s"].min() >= 0


def test_approach_id_unique_after_dedupe():
    df = _load_fixture_df()
    _, approaches = build.process_dataframe(df)
    assert approaches["approach_id"].is_unique


def test_reports_smoke(tmp_path, monkeypatch):
    df = _load_fixture_df()
    objects, approaches = build.process_dataframe(df)

    data_dir = tmp_path / "data/processed"
    data_dir.mkdir(parents=True)
    objects.to_parquet(data_dir / "objects.parquet", index=False)
    approaches.to_parquet(data_dir / "approaches.parquet", index=False)

    monkeypatch.setattr(reports, "DATA_DIR", data_dir)

    outdir = tmp_path / "outputs/reports"
    reports.build_reports(outdir, "Earth")

    assert (outdir / "miss_distance_quantiles.png").exists()
    assert (outdir / "miss_distance_quantiles.html").exists()
    assert (outdir / "miss_distance_ecdf.png").exists()
    assert (outdir / "miss_distance_ecdf.html").exists()
    assert (outdir / "approaches_calendar_heatmap.html").exists()
