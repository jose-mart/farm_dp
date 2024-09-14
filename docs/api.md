API calls

- [POST] - /cows/{id} --> Creates a new cow in the backend
    Body - {"name": "cows_name", "birthdate": "cows_birthdate"}
- [GET]  - /cows/{id} --> Fetch the cow's details and the latest sensor data record
- [POST] - /measurement/{sensor_id}/{cow_id} --> Provides new sensor data for specific measure.
    Body - {"measure_type": "milk_production or weight", "generation_timestamp": "timestamp"}
