# NASA Asteroid Analysis Project

This project analyzes potentially hazardous asteroids and near-Earth objects (NEOs) using NASA's NeoWs (Near Earth Object Web Service) API. It provides tools for collecting, analyzing, and visualizing asteroid data with a focus on close approaches to Earth.

## Features

- Fetch asteroid data from NASA's NeoWs API for the next 15 years
- Identify and analyze potentially hazardous asteroids
- Generate visualizations of asteroid trajectories and characteristics
- Specialized analysis for asteroid Apophis (99942 Apophis (2004 MN4))
- Comprehensive data analysis including size, velocity, and miss distance metrics

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
   pip install pandas matplotlib requests tqdm
   ```

4. Set your NASA API key as an environment variable:
   ```bash
   export NASA_API_KEY="your_api_key_here"
   ```

## Usage

### Collect New Data

Fetch new asteroid data from NASA's API for the next 15 years:

```bash
python neows.py
```

This will create or update the `asteroid_data_full.csv` file with the latest data.

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

## Data Structure

The main dataset (`asteroid_data_full.csv`) contains the following information for each asteroid:

- Identification data (name, ID, JPL URL)
- Physical characteristics (diameter, absolute magnitude)
- Hazard classification (potentially hazardous, sentry object)
- Close approach data (date, velocity, miss distance)
- Orbital information

## Visualizations

The project generates several types of visualizations:

- **Asteroid Trajectories**: Plot showing asteroid close approach distances over time
- **Size vs. Distance**: Scatter plot comparing asteroid size to miss distance
- **Hazardous NEO Analysis**: Scatter plots and yearly trends of hazardous objects
- **Apophis Analysis**: Specialized visualizations focusing on Apophis
- **Dashboard**: Multi-panel visualization with various analyses

## Project Structure

```
nasa-asteroid-analysis/
├── neows.py                 # Data collection from NASA API
├── dangerous_asteroids.py   # Analysis of hazardous asteroids
├── apophis.py              # Specialized Apophis analysis
├── close_approaches.py     # Detailed Apophis close approaches
├── asteroid_data_full.csv  # Collected asteroid data
├── README.md              # This file
└── requirements.txt       # Project dependencies
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