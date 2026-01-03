from datetime import datetime
from pathlib import Path

import pandas as pd

from asteroid_analysis import build
from asteroid_analysis import learning_reports


FIXTURE_PATH = Path("tests/fixtures/asteroid_data_sample.csv")


def test_learning_reports_outputs_and_watchlist_sort(tmp_path):
    input_path = tmp_path / "asteroid_data_sample.csv"
    input_path.write_text(FIXTURE_PATH.read_text())

    processed_dir = tmp_path / "processed"
    build.build_tables(input_path, processed_dir)

    outdir = tmp_path / "learning"
    learning_reports.build_learning_reports(
        outdir=outdir,
        data_dir=processed_dir,
        orbiting_body="Earth",
        as_of_date=datetime(2029, 1, 1),
    )

    watchlist_path = outdir / "watchlist_next_90_days.csv"
    assert watchlist_path.exists()
    watchlist = pd.read_csv(watchlist_path)
    assert watchlist["miss_distance_lunar"].is_monotonic_increasing

    assert (outdir / "near_misses_under_5LD.html").exists()
    assert (outdir / "hazard_vs_size_bins.html").exists()
    assert (outdir / "interpretation_notes.md").exists()
