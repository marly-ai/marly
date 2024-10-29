from pydantic import BaseModel
from typing import List, Dict
from enum import Enum

class ExtractionRequestModel(BaseModel):
    task_id: str
    pdf_key: str
    schemas: List[Dict]
    source_type: str = "pdf"
    destination: str = None

class SchemaResult(BaseModel):
    schema_id: str
    metrics: Dict[str, str]
    schema_data: Dict[str, str]

class ExtractionResponseModel(BaseModel):
    task_id: str
    pdf_key: str
    results: List[SchemaResult]
    source_type: str = "pdf"

class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ModelDetails(BaseModel):
    provider_type: str
    provider_model_name: str
    api_key: str
    markdown_mode: bool
    additional_params: Dict[str, str]
