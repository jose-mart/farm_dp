from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import ClassVar
 

@dataclass_json
@dataclass
class SensorDTO:
    """Sensor validation object"""
    unit: str
    
    ALLOWED_UNITS: ClassVar[set] = {"L", "kg"}

    def __post_init__(self):
        # Validate unit of measurement
        if self.unit not in self.ALLOWED_UNITS:
            raise ValueError(f"Invalid sensor unit: {self.unit}. Allowed values are: {self.ALLOWED_UNITS}.")
