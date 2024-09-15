import pandas as pd
import requests
import json
from datetime import datetime

parquet_file_path = "../../data/sample_data/measurements.parquet"
api_url = "http://localhost:5000/api/measurement" 
headers = {
    "Authorization": "Bearer AuthToken",
    "Content-Type": "application/json"
}

def load_and_insert_measurements():
    df = pd.read_parquet(parquet_file_path)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        measurement_data = {
            "value": row["value"],
            "timestamp": row["timestamp"]
        }

        # Make the API call to insert the measurement
        response = requests.post(
            f"{api_url}/{row['sensor_id']}/{row['cow_id']}",
            headers=headers,
            data=json.dumps(measurement_data)
        )

        if response.status_code == 201:
            print(f"Measurement for sensor {row['sensor_id']} and cow {row['cow_id']} inserted successfully.")
        else:
            print(f"Failed to insert measurement for sensor {row['sensor_id']} and cow {row['cow_id']}. Response: {response.json()}")

if __name__ == "__main__":
    load_and_insert_measurements()
