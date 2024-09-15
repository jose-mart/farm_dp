import pandas as pd
import requests
import json

parquet_file_path = "../../data/sample_data/sensors.parquet"
api_url = "http://localhost:5000/api/sensors"
headers = {
    "Authorization": "Bearer AuthToken",
    "Content-Type": "application/json"
}

def load_and_insert_sensors():
    df = pd.read_parquet(parquet_file_path)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        sensor_data = {
            "unit": row['unit']
        }
        
        # Make the API call to insert the sensor
        response = requests.post(
            f"{api_url}/{row['id']}",
            headers=headers,
            data=json.dumps(sensor_data)
        )

        if response.status_code == 201:
            print(f"Sensor {row['id']} inserted successfully.")
        else:
            print(f"Failed to insert sensor {row['id']}. Response: {response.json()}")

if __name__ == "__main__":
    load_and_insert_sensors()
