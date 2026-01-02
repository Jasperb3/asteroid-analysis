import argparse
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

from asteroid_analysis import ingest
from asteroid_analysis import build as build_mod
from asteroid_analysis import reports as reports_mod


def _default_dates():
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=365 * 15)
    return start_date, end_date


def _print_summary(paths):
    print("\nRun complete.")
    print("Artifacts:")
    for label, path in paths:
        print(f"- {label}: {path}")
    print("\nLaunch the app:")
    print("  streamlit run src/asteroid_analysis/app.py")


def fetch_cmd(args):
    start_date, end_date = _default_dates()
    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    ingest.ingest(
        start_date=start_date,
        end_date=end_date,
        orbiting_body=args.orbiting_body,
        out_path=Path(args.out),
        refresh=args.refresh,
    )
    _print_summary([("CSV", args.out)])


def build_cmd(args):
    build_mod.build_tables(Path(args.input), Path(args.outdir))
    _print_summary(
        [
            ("Objects parquet", Path(args.outdir) / "objects.parquet"),
            ("Approaches parquet", Path(args.outdir) / "approaches.parquet"),
            ("Metadata", Path(args.outdir) / "metadata.json"),
        ]
    )


def reports_cmd(args):
    reports_mod.build_reports(Path(args.outdir), args.orbiting_body)
    _print_summary(
        [
            ("Reports dir", args.outdir),
            ("Quantiles PNG", Path(args.outdir) / "miss_distance_quantiles.png"),
            ("ECDF PNG", Path(args.outdir) / "miss_distance_ecdf.png"),
            ("Heatmap HTML", Path(args.outdir) / "approaches_calendar_heatmap.html"),
        ]
    )


def serve_cmd(_args):
    subprocess.run(
        ["streamlit", "run", "src/asteroid_analysis/app.py"],
        check=False,
    )


def all_cmd(args):
    start_date, end_date = _default_dates()
    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    csv_path = Path(args.out)
    ingest.ingest(
        start_date=start_date,
        end_date=end_date,
        orbiting_body=args.orbiting_body,
        out_path=csv_path,
        refresh=args.refresh,
    )
    build_mod.build_tables(csv_path, Path(args.processed_dir))
    reports_mod.build_reports(Path(args.reports_dir), args.orbiting_body)

    _print_summary(
        [
            ("CSV", csv_path),
            ("Processed dir", args.processed_dir),
            ("Reports dir", args.reports_dir),
        ]
    )


def main():
    parser = argparse.ArgumentParser(description="Asteroid analysis CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch NeoWs feed data.")
    fetch_parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive).")
    fetch_parser.add_argument("--end", help="End date YYYY-MM-DD (inclusive).")
    fetch_parser.add_argument("--orbiting-body", default="Earth")
    fetch_parser.add_argument("--out", default="asteroid_data_full.csv")
    fetch_parser.add_argument("--refresh", action="store_true")
    fetch_parser.set_defaults(func=fetch_cmd)

    build_parser = subparsers.add_parser("build", help="Build processed tables.")
    build_parser.add_argument("--input", default="asteroid_data_full.csv")
    build_parser.add_argument("--outdir", default="data/processed")
    build_parser.set_defaults(func=build_cmd)

    reports_parser = subparsers.add_parser("reports", help="Generate reports.")
    reports_parser.add_argument("--outdir", default="outputs/reports")
    reports_parser.add_argument("--orbiting-body", default="Earth")
    reports_parser.set_defaults(func=reports_cmd)

    serve_parser = subparsers.add_parser("serve", help="Launch Streamlit app.")
    serve_parser.set_defaults(func=serve_cmd)

    all_parser = subparsers.add_parser("all", help="Fetch, build, and report.")
    all_parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive).")
    all_parser.add_argument("--end", help="End date YYYY-MM-DD (inclusive).")
    all_parser.add_argument("--orbiting-body", default="Earth")
    all_parser.add_argument("--out", default="asteroid_data_full.csv")
    all_parser.add_argument("--processed-dir", default="data/processed")
    all_parser.add_argument("--reports-dir", default="outputs/reports")
    all_parser.add_argument("--refresh", action="store_true")
    all_parser.set_defaults(func=all_cmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
