import pandas as pd
import requests

# Load the Excel file
df = pd.read_excel("vehicle_ids.xlsx")

# Extract the vehicle IDs from the "Titans" column
vehicle_ids = df['Titans'].dropna().astype(str)

# Loop through and fetch data
for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)

    if response.status_code == 200:
        with open(f"breadcrumbs_{vehicle_id}.json", "w") as f:
            f.write(response.text)
        print(f"Saved data for vehicle {vehicle_id}")
    else:
        print(f"Failed to fetch for {vehicle_id} (Status: {response.status_code})")
