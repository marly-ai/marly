from pydantic import BaseModel
from typing import Dict, Optional
from uuid import UUID

class TransformationRequestModel(BaseModel):
    metrics: str
    output_data_source: str
    output_data_type: str
    output_filename: Optional[str] = None
    column_locations: Optional[Dict[str, str]] = None
    data_location: str
    prompt_ids: Dict[str, str]
    model_id: UUID
    normalization_id: Optional[str] = None

    class Config:
        protected_namespaces = ()
