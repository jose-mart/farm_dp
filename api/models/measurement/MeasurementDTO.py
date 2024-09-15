from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class MeasurementDTO:
    """Measurement validation object"""
    timestamp: float
    value: float

    def __post_init__(self):
        # Check if timestamp is None
        if self.timestamp is None:
            raise ValueError("timestamp cannot be null.")
        
        # Check if value is negative
        if self.timestamp < 0:
            raise ValueError("timestamp cannot be negative.")
        
        # Check if value is negative
        if self.value < 0:
            raise ValueError("value cannot be negative.")
        
        # Check if value is negative
        if self.value is None:
            raise ValueError("value cannot be null.")