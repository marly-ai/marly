from pydantic import BaseModel
from typing import Dict, Optional

class LoadingResponseModel(BaseModel):
    loading_result: str
    output_data_source: str
    output_data_type: str
    time: int
    output_filename: Optional[str] = None
    data_location: str
    normalization_id: Optional[str] = None
