# NASA Asteroid Analysis Project

## Project Overview

This is a Python-based data analysis project focused on tracking and visualizing potentially hazardous asteroids and near-Earth objects (NEOs). The project uses NASA's NeoWs (Near Earth Object Web Service) API to collect asteroid data and generates various visualizations and analyses of close approaches to Earth.

The project consists of several Python modules that:
1. Fetch asteroid data from NASA's API
2. Analyze potentially hazardous NEOs
3. Generate visualizations of asteroid trajectories and characteristics
4. Create reports and enrich data with orbital information

## Key Components

### Source Modules

- **`src/asteroid_analysis/build.py`**: Main data processing module that processes raw asteroid data, performs data cleaning, and creates objects and approaches dataframes with derived features like logarithmic transformations.
- **`src/asteroid_analysis/reports.py`**: Generates various reports and visualizations including monthly miss distance quantiles, ECDF plots, and calendar heatmaps.
- **`src/asteroid_analysis/features.py`**: Contains functions for feature engineering, including binning and ranking of asteroid characteristics.
- **`src/asteroid_analysis/ingest.py`**: Handles data ingestion from NASA API with caching and chunking capabilities.
- **`src/asteroid_analysis/enrich_orbits.py`**: Fetches and enriches asteroid data with orbital information from NASA's API.
- **`src/asteroid_analysis/app.py`**: Main application entry point that orchestrates the data processing pipeline.

### Data Files

- **`data/processed/`**: Directory containing processed data in parquet format (objects, approaches, orbits).
- **`tests/fixtures/asteroid_data_sample.csv`**: Sample data used for testing purposes.

### Generated Outputs

- **`outputs/reports/`**: Directory containing generated reports and visualizations.

## Dependencies

The project uses several Python libraries:
- `pandas` - For data manipulation and analysis
- `matplotlib` - For generating static visualizations
- `plotly` - For generating interactive visualizations
- `requests` - For API calls to NASA
- `tqdm` - For progress bars during data collection

## Building and Running

### Prerequisites
- Python 3.12 (based on the venv configuration)
- NASA API key stored in the `NASA_API_KEY` environment variable

### Setup
1. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Install required packages (if not already installed):
   ```bash
   pip install -r requirements.txt
   # or if using pyproject.toml:
   pip install -e .
   ```

### Running the Analysis

1. **Ingest new data from NASA API**:
   ```bash
   python -m asteroid_analysis.ingest
   ```

2. **Build processed data tables**:
   ```bash
   python -m asteroid_analysis.build
   ```

3. **Enrich with orbital data**:
   ```bash
   python -m asteroid_analysis.enrich_orbits
   ```

4. **Generate reports**:
   ```bash
   python -m asteroid_analysis.reports
   ```

### Running Tests

To run all tests:
```bash
pytest
```

To run tests with verbose output:
```bash
pytest -v
```

## Development Conventions

- The codebase follows standard Python conventions with type hints
- Tests are organized in the `tests/` directory with one test file per module
- Configuration is handled through command-line arguments using argparse
- Logging is implemented throughout the modules for debugging and monitoring
- Error handling includes retries for API calls with exponential backoff
- Data is stored in parquet format for efficient processing and storage

## Data Structure

The main data processing pipeline creates two primary datasets:
- **Objects**: Contains unique asteroid information (diameter, hazard status, etc.)
- **Approaches**: Contains individual close approach events with associated object data

The processed data includes derived features like logarithmic transformations of distance and diameter, binned categorical variables, and ranking scores for analysis.