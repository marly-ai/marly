from pydantic import BaseModel
from uuid import UUID
from typing import Dict, Optional

class ExtractionRequestModel(BaseModel):
    filename: str
    keywords: Dict[str, str]
    input_data_source: str
    output_data_type: str
    output_data_source: str
    output_filename: Optional[str] = None
    output_mapping: Optional[Dict[str, str]] = None
    data_location: str
    prompt_ids: Dict[str, str]
    model_id: str
    normalization_id: Optional[str] = None

    class Config:
        protected_namespaces = ()
