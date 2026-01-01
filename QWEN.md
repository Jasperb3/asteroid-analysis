# NASA Asteroid Analysis Project

## Project Overview

This is a Python-based data analysis project focused on tracking and visualizing potentially hazardous asteroids and near-Earth objects (NEOs). The project uses NASA's NeoWs (Near Earth Object Web Service) API to collect asteroid data and generates various visualizations and analyses of close approaches to Earth.

The project consists of several Python scripts that:
1. Fetch asteroid data from NASA's API
2. Analyze potentially hazardous asteroids
3. Generate visualizations of asteroid trajectories and characteristics
4. Focus specifically on Apophis (99942 Apophis (2004 MN4)) as a case study

## Key Components

### Scripts

- **`neows.py`**: Main data collection script that fetches asteroid data from NASA's NeoWs API using a 7-day interval over a 15-year period. It processes the data and generates multiple visualizations.
- **`dangerous_asteroids.py`**: Analyzes potentially hazardous NEOs, identifies the closest and largest asteroids, and creates scatter plots and yearly trend visualizations.
- **`apophis.py`**: Specialized analysis focusing on asteroid Apophis, comparing it with other hazardous NEOs.
- **`close_approaches.py`**: Detailed analysis of Apophis close approaches, highlighting the closest approach event.

### Data Files

- **`asteroid_data_full.csv`**: Large dataset (54,612+ records) containing comprehensive asteroid information including:
  - Identification data (name, ID, JPL URL)
  - Physical characteristics (diameter, absolute magnitude)
  - Hazard classification (potentially hazardous, sentry object)
  - Close approach data (date, velocity, miss distance)
  - Orbital information

### Generated Visualizations

- **`asteroid_trajectories.png`**: Plot showing asteroid close approach distances over 15 years
- **`asteroid_size_vs_distance.png`**: Scatter plot of asteroid size vs. miss distance
- **`asteroid_analysis_dashboard.png`**: Multi-panel dashboard with various analyses
- **`hazardous_neos_scatter.png`**: Scatter plot of hazardous NEOs
- **`hazardous_neos_yearly.png`**: Yearly count of hazardous NEO approaches
- **`apophis_close_approaches.png`**: Close approaches visualization for Apophis
- **`apophis_comparison.png`**: Comparison of Apophis with other hazardous NEOs

## Dependencies

The project uses several Python libraries:
- `pandas` - For data manipulation and analysis
- `matplotlib` - For generating visualizations
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
   pip install pandas matplotlib requests tqdm
   ```

### Running the Analysis

1. **Collect new data from NASA API**:
   ```bash
   python neows.py
   ```
   This will fetch asteroid data for the next 15 years and save it to `asteroid_data_full.csv`.

2. **Analyze dangerous asteroids**:
   ```bash
   python dangerous_asteroids.py
   ```
   This will analyze the dataset and generate visualizations.

3. **Run Apophis-specific analysis**:
   ```bash
   python apophis.py
   python close_approaches.py
   ```

## Data Structure

The main dataset (`asteroid_data_full.csv`) contains the following columns:
- `date`: Date of observation
- `id`: Asteroid ID
- `name`: Asteroid name
- `is_potentially_hazardous_asteroid`: Boolean indicating hazard status
- `diameter_km_min/max`: Estimated diameter range in kilometers
- `close_approach_date`: Date of close approach
- `miss_distance_km`: Distance from Earth in kilometers
- `velocity_km_h`: Velocity in km/h
- And many other fields with orbital and physical characteristics

## Development Notes

- All visualization scripts use `matplotlib.use('Agg')` to ensure non-interactive backend for headless environments
- The project includes comprehensive logging and progress indicators
- Data preprocessing includes type conversion and error handling
- Visualizations are saved as high-resolution PNG files (300 DPI) for publication quality