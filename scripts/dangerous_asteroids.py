import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing plt
import matplotlib.pyplot as plt

# Add logging


def main():
    print("Starting asteroid analysis...")
    print(f"Loading data from asteroid_data_full.csv...")

    # Load the CSV data
    df = pd.read_csv("asteroid_data_full.csv")

    # Preprocessing
    # Convert columns using the correct column names
    df['close_approach_date'] = pd.to_datetime(df['close_approach_date'])
    df['miss_distance_km'] = pd.to_numeric(df['miss_distance_km'], errors='coerce')
    df['diameter_km_max'] = pd.to_numeric(df['diameter_km_max'], errors='coerce')
    df['is_potentially_hazardous_asteroid'] = df['is_potentially_hazardous_asteroid'].astype(bool)

    # Filter for hazardous asteroids
    hazardous_neos = df[df['is_potentially_hazardous_asteroid']]

    # Add data info output
    print(f"\nLoaded {len(df)} total asteroids")
    print(f"Found {len(hazardous_neos)} potentially hazardous NEOs")

    # Add error handling and progress info for calculations
    try:
        print("\nCalculating closest approaches...")
        closest_approaches = hazardous_neos.sort_values('miss_distance_km', ascending=True).head(10)

        print("Calculating largest asteroids...")
        largest_asteroids = hazardous_neos.sort_values('diameter_km_max', ascending=False).head(10)
    except Exception as e:
        print(f"Error during calculations: {e}")
        raise

    # Enhanced output formatting
    print("\n=== Top 10 Closest Hazardous NEOs ===")
    pd.set_option('display.float_format', lambda x: '%.2f' % x)  # Format floating point numbers
    print(closest_approaches[['name', 'close_approach_date', 'miss_distance_km', 'diameter_km_max']].to_string())

    print("\n=== Top 10 Largest Hazardous NEOs ===")
    print(largest_asteroids[['name', 'close_approach_date', 'miss_distance_km', 'diameter_km_max']].to_string())

    # Visualization with save functionality
    print("\nGenerating visualizations...")

    plt.figure(figsize=(10, 6))
    plt.scatter(hazardous_neos['miss_distance_km'], hazardous_neos['diameter_km_max'], c='red', label='Hazardous NEOs', alpha=0.7)
    plt.title("Miss Distance vs Diameter (Hazardous NEOs)")
    plt.xlabel("Miss Distance (km)")
    plt.ylabel("Diameter Max (km)")
    plt.legend()
    plt.grid(True)
    plt.savefig('hazardous_neos_scatter.png', dpi=300, bbox_inches='tight')
    print("Saved scatter plot as 'hazardous_neos_scatter.png'")
    plt.close()

    plt.figure(figsize=(10, 6))
    hazardous_neos['year'] = hazardous_neos['close_approach_date'].dt.year
    yearly_counts = hazardous_neos.groupby('year').size()
    yearly_counts.plot(kind='bar', color='orange')
    plt.title("Yearly Count of Hazardous NEO Approaches")
    plt.xlabel("Year")
    plt.ylabel("Count of Hazardous NEOs")
    plt.grid(axis='y')
    plt.savefig('hazardous_neos_yearly.png', dpi=300, bbox_inches='tight')
    print("Saved yearly trend plot as 'hazardous_neos_yearly.png'")
    plt.close()

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
