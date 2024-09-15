from dataclasses import dataclass
from dataclasses_json import dataclass_json
from datetime import date

@dataclass_json
@dataclass
class MeasurementModel:
    sensor_id: str
    cow_id: str
    timestamp: float
    value: float
