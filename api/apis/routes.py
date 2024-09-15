from flask import Blueprint, jsonify, request
from utils.authorization import check_access

from models.cow.CowDTO import CowDTO
from models.cow.CowModel import CowModel
from models.cow.CowService import CowService

from models.measurement.MeasurementModel import MeasurementModel
from models.measurement.MeasurementDTO import MeasurementDTO
from models.measurement.MeasurementService import MeasurementService

from models.sensor.SensorModel import SensorModel
from models.sensor.SensorDTO import SensorDTO
from models.sensor.SensorService import SensorService

from utils.spark_utils import get_spark_session
import json
from datetime import datetime

from utils.config import environment


api_farm = Blueprint("api", __name__)
spark = get_spark_session()

cow_service = CowService(spark)
measurement_service = MeasurementService(spark)
sensor_service = SensorService(spark)


@api_farm.route("/cows/<string:id>",methods=["POST"])
@check_access
def insert_cow(id:str):
    """
    API endpoint to insert a new cow into the backend database.

    Route:
        /cows/<string:id>

    Methods:
        POST

    URL Parameters:
        id (str): The unique identifier for the cow.

    Request Body:
        JSON object containing the cow data, which must match the schema of `CowDTO`.
        Example:
        {
            "name": "Cow1",
            "birthdate": "2023-01-01"
        }

    Headers:
        Authorization: The API requires an authorization header for access control.

    Responses:
        201 Created:
            Description: The cow was successfully added.
            Response Body: JSON object representing the newly created cow.
            Example:
            {
                "id": "123",
                "name": "Cow1",
                "birthdate": "2023-01-01"
            }
        
        400 Bad Request:
            Description: The request was invalid. This can happen in the following cases:
                - No data was provided in the request body.
                - A cow with the specified ID already exists.
                - A required key in the cow data is missing.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "Cow with id 123 already exists."
            }
        
        401 Unauthorized:
            Description: The request was not authorized. The `@check_access` decorator handles this.
            Response Body: None.

        500 Internal Server Error:
            Description: An unexpected error occurred while processing the request.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "An unexpected error occurred."
            }

    Raises:
        KeyError: If a required key is missing in the input data.
        Exception: For any other unexpected errors.
    """
    try:
        # Check if we are receiving request body
        data = request.get_json()
        if not data:
            return jsonify({"message": "No data provided"}), 400
        
        cow_dto = CowDTO.from_json(json.dumps(data))

        # Create Delta table entry with CowDAO
        new_cow = cow_service.add_new_cow(id, cow_dto)

        # If instance is of expected object return object, otherwise fail.
        if isinstance(new_cow, CowModel):
            # Serialize the result
            serialized_cow = new_cow.to_json()

            # Return serialized object
            return jsonify(serialized_cow), 201
        else:
            return jsonify({"message": f"Cow with id {id} already exists."}), 400
    except KeyError as e:
        return jsonify({"message": f"Missing key {str(e)}"}), 400
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    

@api_farm.route("/cows/<string:cow_id>",methods=["GET"])
@check_access
def get_sensor_details(cow_id:str):
    """
    API endpoint to retrieve the latest sensor details for a specific cow.

    Route:
        /cows/<string:cow_id>

    Methods:
        GET

    URL Parameters:
        cow_id (str): The unique identifier for the cow.

    Headers:
        Authorization: The API requires an authorization header for access control.

    Responses:
        201 Created:
            Description: The latest sensor data for the specified cow was successfully retrieved.
            Response Body: JSON object representing the latest sensor measurement.
            Example:
            {
                "cow_id": "123",
                "timestamp": "2024-01-01T12:00:00",
                "measurement_type": "weight",
                "value": 250.0
            }
        
        400 Bad Request:
            Description: The request was invalid. This can happen in the following cases:
                - No cow ID was provided in the URL.
                - No sensor data was found for the specified cow.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "No id provided"
            }

        401 Unauthorized:
            Description: The request was not authorized. The `@check_access` decorator handles this.
            Response Body: None.

        500 Internal Server Error:
            Description: An unexpected error occurred while processing the request.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "An unexpected error occurred."
            }

    Raises:
        KeyError: If a required key is missing during processing.
        Exception: For any other unexpected errors.
    """
    try:
        if not cow_id:
            return jsonify({"message": "No id provided"}), 400
        
        # Get latest measurement for cow_id 
        latest_measurement = cow_service.get_latest_sensor_data(cow_id)
        
        if isinstance(latest_measurement, MeasurementModel):
            # Serialize the result
            serialized_measure = latest_measurement.to_json()
            return jsonify(serialized_measure), 201
        else:
            return jsonify({"message": latest_measurement}), 400
    except KeyError as e:
        return jsonify({"message": f"Missing key {str(e)}"}), 400
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    

@api_farm.route("/measurement/<string:sensor_id>/<string:cow_id>",methods=["POST"])
@check_access
def insert_measurement(sensor_id:str, cow_id:str):
    """
    API endpoint to insert a new measurement for a specific sensor and cow.

    Route:
        /measurement/<string:sensor_id>/<string:cow_id>

    Methods:
        POST

    URL Parameters:
        sensor_id (str): The unique identifier for the sensor.
        cow_id (str): The unique identifier for the cow.

    Headers:
        Authorization: The API requires an authorization header for access control.

    Request Body:
        JSON object representing the measurement details. The required fields depend on the `MeasurementDTO` class.
        Example:
        {
            "timestamp": 1638307200,
            "value": 15.5
        }

    Responses:
        201 Created:
            Description: The new measurement was successfully inserted.
            Response Body: JSON object representing the inserted measurement.
            Example:
            {
                "sensor_id": "sensor123",
                "cow_id": "cow456",
                "timestamp": 1638307200,
                "value": 15.5
            }

        400 Bad Request:
            Description: The request was invalid. This can happen in the following cases:
                - No data was provided in the request body.
                - A required key is missing in the request body.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "No data provided"
            }

        401 Unauthorized:
            Description: The request was not authorized. The `@check_access` decorator handles this.
            Response Body: None.

        500 Internal Server Error:
            Description: An unexpected error occurred while processing the request.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "An unexpected error occurred."
            }

    Raises:
        KeyError: If a required key is missing during processing.
        Exception: For any other unexpected errors.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"message": "No data provided"}), 400

        measurement_dto = MeasurementDTO.from_json(json.dumps(data))

        # Create Delta table entry with MeasurementDAO
        new_measurement = measurement_service.add_new_measure(sensor_id, cow_id, measurement_dto)
        # Serialize the result
        serialized_cow = new_measurement.to_json()

        # Return serialized object
        return jsonify(serialized_cow), 201    
    except KeyError as e:
        return jsonify({"message": f"Missing key {str(e)}"}), 400
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    

@api_farm.route("/sensors/<string:id>",methods=["POST"])
@check_access
def insert_sensor(id:str):
    """
    API endpoint to insert a new sensor record.

    Route:
        /sensors/<string:id>

    Methods:
        POST

    URL Parameters:
        id (str): The unique identifier for the sensor.

    Headers:
        Authorization: The API requires an authorization header for access control.

    Request Body:
        JSON object representing the sensor details. The required fields depend on the `SensorDTO` class.
        Example:
        {
            "unit": "kg"
        }

    Responses:
        201 Created:
            Description: The new sensor was successfully inserted.
            Response Body: JSON object representing the inserted sensor.
            Example:
            {
                "id": "sensor123",
                "unit": "kg"
            }

        400 Bad Request:
            Description: The request was invalid. This can happen in the following cases:
                - No data was provided in the request body.
                - A required key is missing in the request body.
                - The sensor with the specified `id` already exists.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "No data provided"
            }

        401 Unauthorized:
            Description: The request was not authorized. The `@check_access` decorator handles this.
            Response Body: None.

        500 Internal Server Error:
            Description: An unexpected error occurred while processing the request.
            Response Body: JSON object with an error message.
            Example:
            {
                "message": "An unexpected error occurred."
            }

    Raises:
        KeyError: If a required key is missing during processing.
        Exception: For any other unexpected errors.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"message": "No data provided"}), 400
        
        sensor_dto = SensorDTO.from_json(json.dumps(data))

        # Create Delta table entry with SensorDAO
        new_sensor = sensor_service.add_new_sensor(id, sensor_dto)
        if isinstance(new_sensor, SensorModel):
            # Serialize the result
            serialized_cow = new_sensor.to_json()

            # Return serialized object
            return jsonify(serialized_cow), 201
        else:
            return jsonify({"message": f"Sensor with id {id} already exists."}), 400
    except KeyError as e:
        return jsonify({"message": f"Missing key {str(e)}"}), 400
    except Exception as e:
        return jsonify({"message": str(e)}), 500