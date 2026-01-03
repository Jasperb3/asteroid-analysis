from pathlib import Path

import pandas as pd

from asteroid_analysis import app


def test_load_dataframes_empty_does_not_crash(tmp_path):
    data_dir = tmp_path / "processed"
    data_dir.mkdir()

    objects = pd.DataFrame(columns=app.REQUIRED_OBJECT_COLUMNS)
    approaches = pd.DataFrame(columns=app.REQUIRED_APPROACH_COLUMNS)

    objects.to_parquet(data_dir / "objects.parquet", index=False)
    approaches.to_parquet(data_dir / "approaches.parquet", index=False)

    loaded_objects, loaded_approaches, merged, orbits, aggregates = app.load_dataframes(
        data_dir
    )

    assert loaded_objects.empty
    assert loaded_approaches.empty
    assert merged.empty
    assert orbits is None
    assert aggregates is None


def test_get_data_mtimes_updates(tmp_path):
    data_dir = tmp_path / "processed"
    data_dir.mkdir()

    objects = pd.DataFrame(columns=app.REQUIRED_OBJECT_COLUMNS)
    approaches = pd.DataFrame(columns=app.REQUIRED_APPROACH_COLUMNS)

    objects_path = data_dir / "objects.parquet"
    approaches_path = data_dir / "approaches.parquet"
    objects.to_parquet(objects_path, index=False)
    approaches.to_parquet(approaches_path, index=False)

    first_mtimes = app.get_data_mtimes(data_dir)
    objects.to_parquet(objects_path, index=False)
    second_mtimes = app.get_data_mtimes(data_dir)

    assert second_mtimes[0] >= first_mtimes[0]
