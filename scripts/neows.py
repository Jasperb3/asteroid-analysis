import sys
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing pyplot
import matplotlib.pyplot as plt
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from asteroid_analysis import ingest
from asteroid_analysis.features import enrich


def main():
    print("Initializing asteroid data collection...")
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=365 * 15)
    csv_filename = "asteroid_data_full.csv"

    df_full = ingest.ingest(
        start_date=start_date,
        end_date=end_date,
        orbiting_body="Earth",
        out_path=Path(csv_filename),
        refresh=False,
    )

    if df_full.empty:
        print("No data available for plotting.")
        return

    print("\nProcessing visualization dataframe...")
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

    output_file = "asteroid_trajectories.png"
    plt.savefig(output_file)
    print(f"\nPlot saved as: {output_file}")

    print("\nGenerating additional visualizations...")

    plt.figure(figsize=(12, 8))
    plt.scatter(
        df_full['diameter_km_max'],
        df_full['miss_distance_km'],
        c=df_full['is_potentially_hazardous_asteroid'].map({True: 'red', False: 'blue'}),
        alpha=0.6
    )
    plt.xlabel('Asteroid Maximum Diameter (km)')
    plt.ylabel('Miss Distance (km)')
    plt.title('Asteroid Size vs. Miss Distance\n(Red = Potentially Hazardous)')
    plt.yscale('log')
    plt.xscale('log')
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.tight_layout()
    plt.savefig('asteroid_size_vs_distance.png')
    print("Size vs. Distance plot saved as: asteroid_size_vs_distance.png")

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

    ax1.hist(df_full['diameter_km_max'], bins=50, color='skyblue', edgecolor='black')
    ax1.set_title('Distribution of Asteroid Sizes')
    ax1.set_xlabel('Maximum Diameter (km)')
    ax1.set_ylabel('Count')
    ax1.set_xscale('log')

    objects = df_full.drop_duplicates('id')
    hazard_rate_objects = (
        objects['is_potentially_hazardous_asteroid'].sum() / len(objects)
        if len(objects) else 0
    )
    hazard_rate_approaches = (
        df_full['is_potentially_hazardous_asteroid'].sum() / len(df_full)
        if len(df_full) else 0
    )
    print(f"Hazard rate by objects: {hazard_rate_objects:.2%}")
    print(f"Hazard rate by approaches: {hazard_rate_approaches:.2%}")

    enriched = enrich(df_full)
    size_counts = (
        enriched.groupby('size_bin_m', dropna=False)
        .agg(total=('id', 'size'), hazardous=('is_potentially_hazardous_asteroid', 'sum'))
        .reset_index()
    )
    size_counts['hazard_rate'] = size_counts['hazardous'] / size_counts['total']
    ax2.bar(size_counts['size_bin_m'].astype(str), size_counts['hazard_rate'], color='slateblue')
    ax2.set_title('Hazard Rate by Size Bin')
    ax2.set_xlabel('Size Bin')
    ax2.set_ylabel('Hazard Rate')
    ax2.set_ylim(0, 1)

    ax3.boxplot(
        [
            df_full[~df_full['is_potentially_hazardous_asteroid']]['velocity_km_h'],
            df_full[df_full['is_potentially_hazardous_asteroid']]['velocity_km_h']
        ],
        labels=['Safe', 'Hazardous']
    )
    ax3.set_title('Velocity Distribution by Hazard Status')
    ax3.set_ylabel('Velocity (km/h)')

    df_full['month'] = pd.to_datetime(df_full['close_approach_date']).dt.strftime('%Y-%m')
    monthly_counts = df_full['month'].value_counts().sort_index()
    ax4.plot(range(len(monthly_counts)), monthly_counts.values, 'g-')
    ax4.set_title('Monthly Frequency of Close Approaches')
    ax4.set_xlabel('Months from Start')
    ax4.set_ylabel('Number of Approaches')

    plt.tight_layout()
    plt.savefig('asteroid_analysis_dashboard.png')
    print("Analysis dashboard saved as: asteroid_analysis_dashboard.png")

    plt.close('all')


if __name__ == "__main__":
    main()
