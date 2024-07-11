from pydantic import BaseModel
from typing import Dict, Optional

class LoadingRequestModel(BaseModel):
    transformed_metrics: str
    output_data_source: str
    output_data_type: str
    data_location: str
    column_locations: Optional[Dict[str, str]] = None
    output_filename: Optional[str] = None
    normalization_id: Optional[str] = None
    time: int