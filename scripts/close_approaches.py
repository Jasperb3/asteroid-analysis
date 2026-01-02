import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing plt
import matplotlib.pyplot as plt


def main():
    print("Starting close approaches analysis...")
    print("Loading asteroid data...")

    # Load and filter data for Apophis
    df = pd.read_csv("asteroid_data_full.csv")
    apophis_data = df[df['name'] == "99942 Apophis (2004 MN4)"].copy()

    # Convert necessary columns
    apophis_data['close_approach_date'] = pd.to_datetime(apophis_data['close_approach_date'])
    apophis_data['miss_distance_km'] = pd.to_numeric(apophis_data['miss_distance_km'], errors='coerce')
    apophis_data['diameter_km_max'] = pd.to_numeric(apophis_data['diameter_km_max'], errors='coerce')

    print(f"Processing {len(apophis_data)} Apophis close approaches...")

    # Visualization
    print("Generating close approaches visualization...")
    plt.figure(figsize=(10, 6))
    plt.plot(
        apophis_data['close_approach_date'],
        apophis_data['miss_distance_km'],
        marker="o",
        label="Miss Distance (km)"
    )

    # Highlight the closest approach
    closest_approach = apophis_data.loc[apophis_data['miss_distance_km'].idxmin()]
    plt.scatter(
        closest_approach['close_approach_date'],
        closest_approach['miss_distance_km'],
        color="red",
        s=100,
        label=f"Closest Approach ({closest_approach['close_approach_date'].strftime('%Y-%m-%d')})",
        zorder=5
    )

    # Add annotation for closest approach
    plt.annotate(
        f"{closest_approach['miss_distance_km']:,.0f} km",
        (closest_approach['close_approach_date'], closest_approach['miss_distance_km']),
        textcoords="offset points",
        xytext=(-50, -30),
        arrowprops=dict(arrowstyle="->", color="red")
    )

    # Chart Details
    plt.title("Close Approaches of 99942 Apophis (2004 MN4)")
    plt.xlabel("Close Approach Date")
    plt.ylabel("Miss Distance (km)")
    plt.yscale("log")  # Log scale to emphasize proximity differences
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.legend()
    plt.tight_layout()

    # Save the plot
    plt.savefig('apophis_close_approaches.png', dpi=300, bbox_inches='tight')
    print("Saved close approaches plot as 'apophis_close_approaches.png'")
    plt.close()

    # Print detailed information about the closest approach
    print("\n=== Closest Approach Details ===")
    print(closest_approach[['name', 'close_approach_date', 'miss_distance_km', 'diameter_km_max']].to_string())

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
