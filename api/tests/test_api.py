# test_api.py
from utils.config import environment

environment.env = "test"

import json
import pytest
from flask import Flask
from apis.routes import api_farm  
from models.sensor.SensorDTO import SensorDTO  
from models.sensor.SensorModel import SensorModel

import os
import shutil
from utils.spark_utils import get_spark_session, get_config

from pyspark.sql import SparkSession



@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(api_farm)
    
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture(scope="session", autouse=True)
def setup_session():
    reset_sensor_dataset()

@pytest.fixture
def spark():
    with get_spark_session() as mock_spark:
        yield mock_spark

@pytest.fixture
def config():
    yield get_config("test")


def test_insert_sensor(spark: SparkSession, config, client):
    sensor_id = "12345"
    sensor_data = {
        "unit": "kg"
    }
    headers = {
        "Authorization": "Bearer AuthToken"
    }

    response = client.post(
        f"/sensors/{sensor_id}",
        data=json.dumps(sensor_data),
        content_type='application/json',
        headers=headers
    )

    df = spark.read.format("delta").load(f"{config["bronze_path"]}sensors")

    assert response.status_code == 201
    response_data = json.loads(response.get_json())
    assert response_data["id"] == sensor_id
    assert response_data["unit"] == "kg"
    assert df.count() == 1


def reset_sensor_dataset():
    target_folder = "../../data/farm_testing/"

    if os.path.exists(target_folder):
        shutil.rmtree(target_folder)


if __name__ == "__main__":
    pytest.main()