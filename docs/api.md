# API Design Document
## Overview
The API provides endpoints to manage and interact with data related to cows, sensors, and measurements. It allows clients to insert and retrieve records for cows and sensors, as well as to record measurements. The API is built using Flask and is designed to handle JSON data in a RESTful manner.

## Architecture
### Components

- **API Layer**: Exposes RESTful endpoints for interacting with cow, sensor, and measurement data.
- **Service Layer**: Implements business logic and interacts with Data Access Objects (DAOs) to perform data operations.
- **Data Access Objects** (DAOs): Manage read and write operations to Delta Lake tables.
- **Data Transfer Objects** (DTOs): Validate and serialize/deserialize data for API interactions.
- **Data Models**: Define the schema and structure for data storage and retrieval.

## Workflow
1. **Incoming Requests**: API endpoints receive and validate data, then pass it to the service layer.
2. **Business Logic**: Service layer processes the data and interacts with DAOs to perform operations.
3. **Data Storage**: DAOs write data to Delta Lake or retrieve data from it based on the requested operations.
4. **Response Generation**: Data is serialized into JSON format and sent back to the client.

## Endpoints
### Insert Cow

- **URL**: /cows/<string:id>
- **Method**: POST
- **Description**: Inserts a new cow record into the system.
- **Request Body**: JSON object containing name (string) and birthdate (date in 'YYYY-MM-DD' format).
- **Responses**:
    - **201** Created with the serialized cow object if the cow is successfully added.
    - **400** Bad Request if the cow with the given ID already exists or if there is a validation error.
    - **500** Internal Server Error for unexpected errors.
- **Error Handling**: Handles missing data and format errors. Returns appropriate messages and HTTP status codes.

### Get Sensor Details

- **URL**: /cows/<string:cow_id>
- **Method**: GET
- **Description**: Retrieves the latest sensor data for a specified cow.
- **Request Parameters**: cow_id (string) - The ID of the cow for which to retrieve sensor data.
- **Responses**:
    - **201** Created with the serialized sensor data if found.
    - **400** Bad Request if no records are found for the provided cow_id or if cow_id is missing.
    - **500** Internal Server Error for unexpected errors.
- **Error Handling**: Returns appropriate messages and HTTP status codes for missing or incorrect parameters.

### Insert Measurement

- **URL**: /measurement/<string:sensor_id>/<string:cow_id>
- **Method**: POST
- **Description**: Records a new measurement for a given sensor and cow.
- **Request Body**: JSON object containing measurement details, including timestamp (float) and value (float).
- **Responses**:
    - **201** Created with the serialized measurement data if the measurement is successfully recorded.
    - **400** Bad Request if no data is provided or if there's an error in the request body.
    - **500** Internal Server Error for unexpected errors.
- **Error Handling**: Handles missing data and format errors. Returns appropriate messages and HTTP status codes.

### Insert Sensor

- **URL**: /sensors/<string:id>
- **Method**: POST
- **Description**: Adds a new sensor record.
- **Request Body**: JSON object containing sensor details, typically including attributes relevant to sensors.
- **Responses**:
    - **201** Created with the serialized sensor object if the sensor is successfully added.
    - **400** Bad Request if the sensor with the given ID already exists or if there is a validation error.
    - **500** Internal Server Error for unexpected errors.
- **Error Handling**: Handles missing data and format errors. Returns appropriate messages and HTTP status codes.

## Error Handling
- **Key Errors**: Handled for missing required fields in the request body.
- **Format Errors**: Handled for incorrect data formats (e.g., date formats).
- **Conflict Errors**: Returned if attempting to create a duplicate record.
- **Server Errors**: Captures unexpected issues and provides a generic error message.

## Security
- **Access Control**: Utilizes the @check_access decorator to ensure that only authorized users can access the endpoints.

## Data Validation
- **DTOs**: Use Data Transfer Objects (DTOs) like CowDTO, SensorDTO, and MeasurementDTO for input validation and serialization.
- **Data Conversion**: Handles conversion of strings to date objects and other necessary formats.

## Implementation Notes
- **Data Storage**: Utilizes Delta Lake for persistent storage of cow, sensor, and measurement data.
- **Response Formats**: JSON is used for both requests and responses, ensuring compatibility with web and mobile clients.

This design ensures a clear and structured approach to managing and interacting with cow-related data, providing robust error handling and data validation to maintain data integrity and API reliability.