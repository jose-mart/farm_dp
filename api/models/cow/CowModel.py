from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import date

@dataclass_json
@dataclass
class CowModel:
    id: str
    name: str
    birthdate: date = field(metadata=config(encoder=lambda d: d.isoformat() if d else None)) # Encode birthdate as a serializable object to isoformat

