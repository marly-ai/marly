from pydantic import BaseModel
from typing import List, Dict
from enum import Enum

class TransformationRequestModel(BaseModel):
    task_id: str
    pdf_key: str
    results: List['SchemaResult']
    source_type: str = "pdf"
    destination: str = None

class TransformationOnlyRequestModel(BaseModel):
    task_id: str
    data_location_key: str
    schemas: List[str]
    destination: str
    raw_data: str

class TransformationResponseModel(BaseModel):
    task_id: str
    pdf_key: str
    results: List['SchemaResult']

class SchemaResult(BaseModel):
    schema_id: str
    metrics: Dict[str, str]
    schema_data: Dict[str, str]

class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ExtractionResponseModel(BaseModel):
    task_id: str
    pdf_key: str
    results: List[SchemaResult]

class ModelDetails(BaseModel):
    provider_type: str
    provider_model_name: str
    api_key: str
    markdown_mode: bool
    additional_params: Dict[str, str]
