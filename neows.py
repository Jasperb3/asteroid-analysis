import os
import requests
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing pyplot
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from tqdm import tqdm

# Function to query the NeoWs API
def fetch_asteroid_data(start_date, end_date, api_key):
    base_url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "api_key": api_key,
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")

def flatten_asteroid_data(asteroid, date):
    """Flatten the nested asteroid data structure into a single dictionary."""
    flat_data = {
        'date': date,
        'id': asteroid.get('id'),
        'neo_reference_id': asteroid.get('neo_reference_id'),
        'name': asteroid.get('name'),
        'nasa_jpl_url': asteroid.get('nasa_jpl_url'),
        'absolute_magnitude_h': asteroid.get('absolute_magnitude_h'),
        'is_potentially_hazardous_asteroid': asteroid.get('is_potentially_hazardous_asteroid'),
        'is_sentry_object': asteroid.get('is_sentry_object'),
        
        # Estimated diameter fields
        'diameter_km_min': asteroid.get('estimated_diameter', {}).get('kilometers', {}).get('estimated_diameter_min'),
        'diameter_km_max': asteroid.get('estimated_diameter', {}).get('kilometers', {}).get('estimated_diameter_max'),
        'diameter_m_min': asteroid.get('estimated_diameter', {}).get('meters', {}).get('estimated_diameter_min'),
        'diameter_m_max': asteroid.get('estimated_diameter', {}).get('meters', {}).get('estimated_diameter_max'),
    }
    
    # Add close approach data
    close_approach = asteroid.get('close_approach_data', [{}])[0]  # Get first approach
    flat_data.update({
        'close_approach_date': close_approach.get('close_approach_date'),
        'close_approach_date_full': close_approach.get('close_approach_date_full'),
        'epoch_date_close_approach': close_approach.get('epoch_date_close_approach'),
        
        # Velocity data
        'velocity_km_s': float(close_approach.get('relative_velocity', {}).get('kilometers_per_second', 0)),
        'velocity_km_h': float(close_approach.get('relative_velocity', {}).get('kilometers_per_hour', 0)),
        'velocity_mph': float(close_approach.get('relative_velocity', {}).get('miles_per_hour', 0)),
        
        # Miss distance data
        'miss_distance_astronomical': float(close_approach.get('miss_distance', {}).get('astronomical', 0)),
        'miss_distance_lunar': float(close_approach.get('miss_distance', {}).get('lunar', 0)),
        'miss_distance_km': float(close_approach.get('miss_distance', {}).get('kilometers', 0)),
        'miss_distance_miles': float(close_approach.get('miss_distance', {}).get('miles', 0)),
        
        'orbiting_body': close_approach.get('orbiting_body')
    })
    
    return flat_data

# Initialize parameters
print("Initializing asteroid data collection...")
api_key = os.getenv("NASA_API_KEY")
if not api_key:
    raise ValueError("NASA_API_KEY environment variable not set")

start_date = datetime.now()
end_date = start_date + timedelta(days=365 * 15)
data_points = []

# Calculate total number of iterations for progress bar
total_days = (end_date - start_date).days
num_iterations = (total_days // 7) + 1

# Modified data collection loop
print(f"\nFetching asteroid data from {start_date.date()} to {end_date.date()}")
current_date = start_date
with tqdm(total=num_iterations, desc="Collecting data") as pbar:
    while current_date < end_date:
        next_date = current_date + timedelta(days=7)
        if next_date > end_date:
            next_date = end_date
            
        try:
            asteroid_data = fetch_asteroid_data(
                current_date.strftime("%Y-%m-%d"), 
                next_date.strftime("%Y-%m-%d"), 
                api_key
            )
            
            # Extract all available data
            for date, asteroids in asteroid_data.get("near_earth_objects", {}).items():
                for asteroid in asteroids:
                    data_points.append(flatten_asteroid_data(asteroid, date))
            
            current_date = next_date + timedelta(days=1)
            pbar.update(1)
            
        except Exception as e:
            print(f"\nError fetching data: {e}")
            break

print(f"\nProcessing {len(data_points)} asteroid observations...")

# Create and save the full DataFrame
df_full = pd.DataFrame(data_points)
csv_filename = "asteroid_data_full.csv"
df_full.to_csv(csv_filename, index=False)
print(f"Full dataset saved to: {csv_filename}")

# Create the visualization DataFrame (simplified for plotting)
df_plot = df_full[['name', 'close_approach_date', 'miss_distance_km']].copy()
df_plot['close_approach_date'] = pd.to_datetime(df_plot['close_approach_date'])

print("Generating plot...")
plt.figure(figsize=(12, 6))
for name, group in df_plot.groupby('name'):
    plt.plot(group['close_approach_date'], group['miss_distance_km'], label=name, alpha=0.6)

plt.title("Asteroid Close Approach Distances Over the Next 15 Years")
plt.xlabel("Date")
plt.ylabel("Miss Distance from Earth (km)")
plt.legend(loc='upper left', bbox_to_anchor=(1, 1), title="Asteroid Name")
plt.tight_layout()

# Save plot instead of showing it
output_file = "asteroid_trajectories.png"
plt.savefig(output_file)
print(f"\nPlot saved as: {output_file}")

print("\nGenerating additional visualizations...")

# 1. Scatter plot of asteroid size vs. miss distance with hazard indication
plt.figure(figsize=(12, 8))
plt.scatter(df_full['diameter_km_max'], 
           df_full['miss_distance_km'],
           c=df_full['is_potentially_hazardous_asteroid'].map({True: 'red', False: 'blue'}),
           alpha=0.6)
plt.xlabel('Asteroid Maximum Diameter (km)')
plt.ylabel('Miss Distance (km)')
plt.title('Asteroid Size vs. Miss Distance\n(Red = Potentially Hazardous)')
plt.yscale('log')  # Log scale for better visualization of distances
plt.xscale('log')  # Log scale for better visualization of sizes
plt.grid(True, which="both", ls="-", alpha=0.2)
plt.tight_layout()
plt.savefig('asteroid_size_vs_distance.png')
print("Size vs. Distance plot saved as: asteroid_size_vs_distance.png")

# 2. Multiple subplot visualization
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

# Histogram of asteroid sizes
ax1.hist(df_full['diameter_km_max'], bins=50, color='skyblue', edgecolor='black')
ax1.set_title('Distribution of Asteroid Sizes')
ax1.set_xlabel('Maximum Diameter (km)')
ax1.set_ylabel('Count')
ax1.set_xscale('log')

# Pie chart of hazardous vs non-hazardous
hazard_counts = df_full['is_potentially_hazardous_asteroid'].value_counts()
ax2.pie(hazard_counts, labels=['Safe', 'Hazardous'], colors=['green', 'red'],
        autopct='%1.1f%%', explode=(0, 0.1))
ax2.set_title('Proportion of Potentially Hazardous Asteroids')

# Box plot of velocities by hazard status
ax3.boxplot([
    df_full[~df_full['is_potentially_hazardous_asteroid']]['velocity_km_h'],
    df_full[df_full['is_potentially_hazardous_asteroid']]['velocity_km_h']
], labels=['Safe', 'Hazardous'])
ax3.set_title('Velocity Distribution by Hazard Status')
ax3.set_ylabel('Velocity (km/h)')

# Monthly approach frequency
df_full['month'] = pd.to_datetime(df_full['close_approach_date']).dt.strftime('%Y-%m')
monthly_counts = df_full['month'].value_counts().sort_index()
ax4.plot(range(len(monthly_counts)), monthly_counts.values, 'g-')
ax4.set_title('Monthly Frequency of Close Approaches')
ax4.set_xlabel('Months from Start')
ax4.set_ylabel('Number of Approaches')

plt.tight_layout()
plt.savefig('asteroid_analysis_dashboard.png')
print("Analysis dashboard saved as: asteroid_analysis_dashboard.png")

# Clear the current figure
plt.close('all')
