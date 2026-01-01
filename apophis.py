import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing plt
import matplotlib.pyplot as plt

print("Starting Apophis analysis...")
print("Loading asteroid data...")

# Load the CSV data
hazardous_neos = pd.read_csv("asteroid_data_full.csv")

# Convert necessary columns
hazardous_neos['close_approach_date'] = pd.to_datetime(hazardous_neos['close_approach_date'])
hazardous_neos['miss_distance_km'] = pd.to_numeric(hazardous_neos['miss_distance_km'], errors='coerce')
hazardous_neos['diameter_km_max'] = pd.to_numeric(hazardous_neos['diameter_km_max'], errors='coerce')
hazardous_neos = hazardous_neos[hazardous_neos['is_potentially_hazardous_asteroid']]

print(f"Processing {len(hazardous_neos)} hazardous NEOs...")

# Highlight Apophis in the dataset
hazardous_neos['is_apophis'] = hazardous_neos['name'] == "99942 Apophis (2004 MN4)"

print("Generating Apophis comparison plot...")
plt.figure(figsize=(10, 6))
plt.scatter(
    hazardous_neos['miss_distance_km'],
    hazardous_neos['diameter_km_max'],
    c=hazardous_neos['is_apophis'].map({True: 'red', False: 'blue'}),
    label="NEOs",
    alpha=0.7
)

# Find Apophis data point for annotation
apophis_data = hazardous_neos[hazardous_neos['is_apophis']].iloc[0]
plt.annotate(
    "Apophis (2029)", 
    (apophis_data['miss_distance_km'], apophis_data['diameter_km_max']), 
    textcoords="offset points", 
    xytext=(-40, 10), 
    arrowprops=dict(arrowstyle="->", color='red')
)

plt.title("Comparison of Hazardous NEOs by Miss Distance and Size")
plt.xlabel("Miss Distance (km)")
plt.ylabel("Estimated Diameter Max (km)")
plt.grid(True)
plt.legend(["Apophis (Highlighted)", "Other Hazardous NEOs"], loc="upper right")
plt.yscale('log')  # Add log scale for better visualization
plt.xscale('log')  # Add log scale for better visualization

plt.savefig('apophis_comparison.png', dpi=300, bbox_inches='tight')
print("Saved Apophis comparison plot as 'apophis_comparison.png'")
plt.close()

# Print Apophis specific information
print("\n=== Apophis Details ===")
apophis_details = hazardous_neos[hazardous_neos['is_apophis']]
print(apophis_details[['name', 'close_approach_date', 'miss_distance_km', 'diameter_km_max']].to_string())