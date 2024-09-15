import pandas as pd
import requests
import json

parquet_file_path = "../../data/sample_data/cows.parquet"
api_url = "http://localhost:5000/api/cows"  
headers = {
    "Authorization": "Bearer AuthToken", 
    "Content-Type": "application/json"
}

def load_and_insert_cows():
    df = pd.read_parquet(parquet_file_path)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        cow_data = {
            "name": row['name'],
            "birthdate": row['birthdate'].isoformat() if pd.notnull(row['birthdate']) else None
        }
        
        # Make the API call to insert the cow
        response = requests.post(
            f"{api_url}/{row['id']}",
            headers=headers,
            data=json.dumps(cow_data) 
        )

        if response.status_code == 201:
            print(f"Cow {row['id']} inserted successfully.")
        else:
            print(f"Failed to insert cow {row['id']}. Response: {response.json()}")

if __name__ == "__main__":
    load_and_insert_cows()
