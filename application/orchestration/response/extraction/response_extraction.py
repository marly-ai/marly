from pydantic import BaseModel
from typing import Dict, Optional

class ExtractionResponseModel(BaseModel):
    metrics: str
    output_data_source: str
    output_data_type: str
    time: int
    output_filename: Optional[str] = None
    column_locations: Optional[Dict[str, str]] = None
    data_location: str
    prompt_ids: Dict[str, str]
    model_id: str
    normalization_id: Optional[str] = None
