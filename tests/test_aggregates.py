from pathlib import Path

import pandas as pd

from asteroid_analysis import build


FIXTURE_PATH = Path("tests/fixtures/asteroid_data_sample.csv")


def test_aggregates_created_and_schema(tmp_path):
    input_path = tmp_path / "asteroid_data_sample.csv"
    input_path.write_text(FIXTURE_PATH.read_text())

    outdir = tmp_path / "processed"
    build.build_tables(input_path, outdir)

    aggregates_path = outdir / "aggregates.parquet"
    assert aggregates_path.exists()

    aggregates = pd.read_parquet(aggregates_path)
    assert "aggregate_type" in aggregates.columns
    assert "orbiting_body" in aggregates.columns
