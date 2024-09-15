from dataclasses import dataclass
from dataclasses_json import dataclass_json
from datetime import date, datetime

@dataclass_json
@dataclass
class CowDTO:
    """Cow validation object"""
    name: str
    birthdate: date

    def __post_init__(self):
        # If birthdate is a string, attempt to convert it to a date object
        if isinstance(self.birthdate, str):
            try:
                # Attempt to parse the string in the format 'YYYY-MM-DD'
                self.birthdate = datetime.strptime(self.birthdate, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"birthdate string is not in the correct format 'YYYY-MM-DD': {self.birthdate}")
            
        if not isinstance(self.birthdate, date):
            raise TypeError(f"birthdate must be of type 'date', got {type(self.birthdate).__name__} instead.")
        
        # Validate that birthdate is not in the future
        if self.birthdate > date.today():
            raise ValueError(f"birthdate cannot be in the future. Got {self.birthdate}.")