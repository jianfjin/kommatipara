from pydantic import BaseModel, Field, ConfigDict, TypeAdapter, ValidationError
from typing import Dict, List, Optional

class DatasetConfig(BaseModel):
    """
    Load the configuration json data to the model and validate the data.
    """
    name: str
    format: str
    path: str
    sep: str
    schema: Dict[str, str]
    filters: Optional[Dict[str, List[str]]] = None
    exclude: Optional[List[str]] = None
    rename: Optional[Dict[str, str]] = None

    def update(self, **new_data):
        for field, value in new_data.items():
            setattr(self, field, value)