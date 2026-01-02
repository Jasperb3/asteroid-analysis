# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Python analysis toolkit with a package in `src/`.
- `src/asteroid_analysis/` contains ingestion, build, reports, app, and CLI modules.
- `scripts/` holds script entrypoints; root wrappers (`neows.py`, etc.) remain for compatibility.
- `data/raw/` stores cached API responses; `data/processed/` stores parquet outputs.
- `outputs/` contains report artifacts and runtime metadata.

## Build, Test, and Development Commands
- `python -m venv .venv` and `source .venv/bin/activate` to create/activate a local environment.
- `pip install -e .[dev]` to install runtime + dev dependencies.
- `python neows.py` to fetch/update `asteroid_data_full.csv` (requires `NASA_API_KEY`).
- `python -m asteroid_analysis.build --input asteroid_data_full.csv --outdir data/processed`.
- `python -m asteroid_analysis.reports --outdir outputs/reports --orbiting-body Earth`.
- `streamlit run src/asteroid_analysis/app.py` to launch the dashboard.
- `python -m asteroid_analysis.cli all` for fetch → build → reports.

## Coding Style & Naming Conventions
- Python 3.12+ compatible code.
- Use 4-space indentation and PEP 8-style naming (`snake_case` for functions/vars).
- Prefer modifying package modules in `src/asteroid_analysis/`.
- Tooling is configured in `pyproject.toml` (ruff, mypy, pytest).

## Testing Guidelines
- Tests use `pytest` in the `tests/` directory (`test_*.py`).
- Use the fixture CSV in `tests/fixtures/` to keep tests fast and offline.

## Commit & Pull Request Guidelines
- Git history is minimal (single “Initial commit”); no established commit format.
- Keep commits focused and descriptive (e.g., “Add Apophis plot annotations”).
- PRs should describe the change, list any new dependencies, and include example
  outputs (e.g., generated plots) when relevant.

## Security & Configuration Tips
- Set `NASA_API_KEY` in your shell before running data collection:
  `export NASA_API_KEY="your_key_here"`.
- Avoid committing API keys or large generated datasets unless requested.
