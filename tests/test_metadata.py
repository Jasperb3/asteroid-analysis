import json
from pathlib import Path

from asteroid_analysis import build
from asteroid_analysis.metadata import _hash_file


FIXTURE_PATH = Path("tests/fixtures/asteroid_data_sample.csv")


def test_input_hash_stable(tmp_path):
    sample_path = tmp_path / "asteroid_data_sample.csv"
    sample_path.write_text(FIXTURE_PATH.read_text())

    first_hash = _hash_file(sample_path)
    second_hash = _hash_file(sample_path)

    assert first_hash == second_hash
    assert first_hash != ""


def test_metadata_duplicate_count_matches_warning(tmp_path):
    input_path = tmp_path / "asteroid_data_sample.csv"
    input_path.write_text(FIXTURE_PATH.read_text())

    outdir = tmp_path / "processed"
    build.build_tables(input_path, outdir)

    metadata = json.loads((outdir / "metadata.json").read_text())
    assert metadata["duplicate_approach_id_count"] == 1
    assert metadata["input_csv_hash"] == _hash_file(input_path)
