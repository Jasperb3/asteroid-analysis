# NASA Asteroid Analysis Project

This project analyzes potentially hazardous asteroids and near-Earth objects (NEOs) using NASA's NeoWs (Near Earth Object Web Service) API. It provides tools for collecting, analyzing, and visualizing asteroid data with a focus on close approaches to Earth.

## Features

- Fetch asteroid data from NASA's NeoWs API for the next 15 years
- Identify and analyze potentially hazardous asteroids
- Generate visualizations of asteroid trajectories and characteristics
- Specialized analysis for asteroid Apophis (99942 Apophis (2004 MN4))
- Comprehensive data analysis including size, velocity, and miss distance metrics
- Cached ingestion with chunk reuse and failure logging
- Processed tables, reports, and metadata for large datasets
- Interactive Streamlit dashboard with filters and rankings
- Optional orbit enrichment for orbit-class and MOID analysis

## Requirements

- Python 3.12+
- NASA API key (get one at [NASA API Portal](https://api.nasa.gov/))

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nasa-asteroid-analysis.git
   cd nasa-asteroid-analysis
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -e .
   ```

4. Set your NASA API key as an environment variable:
   ```bash
   export NASA_API_KEY="your_api_key_here"
   ```

## Usage

### Recommended Workflow (Quick Start)

1. Fetch cached data:
   ```bash
   python -m asteroid_analysis.ingest --start 2024-01-01 --end 2039-12-31 --orbiting-body Earth --out asteroid_data_full.csv
   ```
2. Build processed tables:
   ```bash
   python -m asteroid_analysis.build --input asteroid_data_full.csv --outdir data/processed
   ```
3. Generate reports:
   ```bash
   python -m asteroid_analysis.reports --outdir outputs/reports --orbiting-body Earth
   ```
4. Launch the dashboard:
   ```bash
   streamlit run src/asteroid_analysis/app.py
   ```

### Collect New Data

Fetch new asteroid data from NASA's API for the next 15 years:

```bash
python neows.py
```

This will create or update the `asteroid_data_full.csv` file with the latest data.

You can also run cached ingestion directly:

```bash
python -m asteroid_analysis.ingest --start 2024-01-01 --end 2039-12-31 --orbiting-body Earth --out asteroid_data_full.csv
```

### Analyze Hazardous Asteroids

Analyze the collected data to identify the most dangerous asteroids:

```bash
python dangerous_asteroids.py
```

This will output statistics about the closest and largest hazardous asteroids, and generate visualizations.

### Apophis Analysis

Run specialized analysis for asteroid Apophis:

```bash
python apophis.py
python close_approaches.py
```

These scripts provide detailed analysis and visualizations specifically for Apophis.

### Build Processed Tables

```bash
python -m asteroid_analysis.build --input asteroid_data_full.csv --outdir data/processed
```

This writes `objects.parquet`, `approaches.parquet`, and `metadata.json`.

### Generate Reports

```bash
python -m asteroid_analysis.reports --outdir outputs/reports --orbiting-body Earth --data-dir data/processed
```

### Learning Reports

```bash
python -m asteroid_analysis.learning_reports --data-dir data/processed --outdir outputs/learning --orbiting-body Earth
```

Outputs include a 90-day watchlist, near-miss storyboard, hazard vs size bins, and interpretation notes.

### Optional Orbit Enrichment

```bash
python -m asteroid_analysis.enrich_orbits --out data/processed/orbits.parquet
```

### Interactive Dashboard

```bash
streamlit run src/asteroid_analysis/app.py
```

### One-Command Pipeline

```bash
python -m asteroid_analysis.cli all --start 2024-01-01 --end 2039-12-31 --orbiting-body Earth --raw-dir data/raw --processed-dir data/processed --reports-dir outputs/reports
```

## Development

Setup:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Common commands:

- `make lint` (ruff checks)
- `make format` (ruff formatter)
- `make test` (pytest)
- `make run-app` (launch Streamlit dashboard)

Streamlit dashboard:

```bash
python -m asteroid_analysis.build --input asteroid_data_full.csv --outdir data/processed
streamlit run src/asteroid_analysis/app.py
```

Custom output directories:

```bash
python -m asteroid_analysis.build --input asteroid_data_full.csv --outdir data/processed_custom
python -m asteroid_analysis.reports --outdir outputs/reports --orbiting-body Earth --data-dir data/processed_custom
```

Pipeline CLI:

```bash
python -m asteroid_analysis.cli all --start 2024-01-01 --end 2039-12-31 --orbiting-body Earth --raw-dir data/raw --processed-dir data/processed --reports-dir outputs/reports
```

## Data Structure

The main dataset (`asteroid_data_full.csv`) contains the following information for each asteroid:

- Identification data (name, ID, JPL URL)
- Physical characteristics (diameter, absolute magnitude)
- Hazard classification (potentially hazardous, sentry object)
- Close approach data (date, velocity, miss distance)
- Orbital information

## Data Model

- Approach-level rows: each row represents one close-approach event. The same object can appear multiple times across dates.
- Object-level rows: the `objects` table in `data/processed/objects.*` is deduplicated by `id`.
- Counts: “approaches” counts are based on the approach table; “objects” counts are based on unique object ids.
- Metadata: `data/processed/metadata.json` and `outputs/metadata.json` capture run context and coverage.

## Visualizations

The project generates several types of visualizations:

- **Asteroid Trajectories**: Plot showing asteroid close approach distances over time
- **Size vs. Distance**: Scatter plot comparing asteroid size to miss distance
- **Hazardous NEO Analysis**: Scatter plots and yearly trends of hazardous objects
- **Apophis Analysis**: Specialized visualizations focusing on Apophis
- **Dashboard**: Multi-panel visualization with various analyses
- **Reports**: Quantile bands, ECDFs, and calendar heatmaps for large datasets

## FAQ

**Do I need an API key?**  
Yes. Set `NASA_API_KEY` in your environment before running ingestion or orbit enrichment.

**Why does ingestion skip some chunks?**  
Chunks are cached in `data/raw/`. Use `--refresh` to re-fetch a cached range.

**The dashboard fails to load or shows no data.**  
Run the build step first so `data/processed/objects.parquet` and `data/processed/approaches.parquet` exist.

**Why are there fewer objects than approaches?**  
Approaches are event rows; objects are deduplicated by `id` in the processed table.

**Can I include non-Earth approaches?**  
Use `--orbiting-body all` (ingest) or select a different body in the dashboard filters.

## Project Structure

```
nasa-asteroid-analysis/
├── neows.py                 # Data collection from NASA API
├── dangerous_asteroids.py   # Analysis of hazardous asteroids
├── apophis.py              # Specialized Apophis analysis
├── close_approaches.py     # Detailed Apophis close approaches
├── asteroid_data_full.csv  # Collected asteroid data
├── README.md              # This file
├── src/asteroid_analysis/  # Package code (ingest, build, reports, app)
├── scripts/               # Script entrypoints
└── tests/                 # Pytest suite
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is open source and available under the [MIT License](LICENSE).

## Acknowledgments

- Data provided by [NASA's NeoWs API](https://api.nasa.gov/)
- Special thanks to the NASA Open APIs program
