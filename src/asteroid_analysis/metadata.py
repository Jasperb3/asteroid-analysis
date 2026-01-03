from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import json
import hashlib


@dataclass
class RunMetadata:
    generated_at: str
    input_path: str
    input_csv_hash: str
    raw_cache_dir: str
    date_min: str
    date_max: str
    total_approaches: int
    unique_objects: int
    hazardous_objects: int
    hazardous_approaches: int
    sentry_objects: int
    duplicate_approach_id_count: int
    orbiting_body_filter: str
    notes: str

    def to_dict(self):
        return asdict(self)


def build_metadata(
    df: pd.DataFrame,
    input_path: Path,
    orbiting_body_filter: str,
    input_csv_hash: str,
    raw_cache_dir: str,
    duplicate_approach_id_count: int,
) -> RunMetadata:
    date_min = df["close_approach_date"].min()
    date_max = df["close_approach_date"].max()
    objects = df.drop_duplicates("id")
    notes = "each row is one close-approach event; object may appear multiple times"

    return RunMetadata(
        generated_at=datetime.now(timezone.utc).isoformat(),
        input_path=str(input_path),
        input_csv_hash=input_csv_hash,
        raw_cache_dir=raw_cache_dir,
        date_min=str(date_min) if pd.notna(date_min) else "",
        date_max=str(date_max) if pd.notna(date_max) else "",
        total_approaches=int(len(df)),
        unique_objects=int(df["id"].nunique()),
        hazardous_objects=int(objects["is_potentially_hazardous_asteroid"].sum()),
        hazardous_approaches=int(df["is_potentially_hazardous_asteroid"].sum()),
        sentry_objects=int(objects["is_sentry_object"].sum()),
        duplicate_approach_id_count=duplicate_approach_id_count,
        orbiting_body_filter=orbiting_body_filter,
        notes=notes,
    )


def write_metadata(metadata: RunMetadata, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metadata.to_dict(), indent=2))


def _hash_file(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
