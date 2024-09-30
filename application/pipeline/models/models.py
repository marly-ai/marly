from pydantic import BaseModel, Field
from typing import List, Dict, Any
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class WorkloadItem(BaseModel):
    pdf_stream: str = Field(default=None)
    schemas: List[str]
    data_source: str = Field(default=None)
    documents_location: str = Field(default=None)
    file_name: str = Field(default=None)
    additional_params: Dict[str, Any] = Field(default_factory=dict)

class PipelineRequestModel(BaseModel):
    workloads: List[WorkloadItem]
    provider_type: str
    provider_model_name: str
    api_key: str
    markdown_mode: bool = False
    additional_params: Dict[str, Any] = Field(default_factory=dict)

class PipelineResponseModel(BaseModel):
    message: str
    task_id: str

class PipelineResult(BaseModel):
    task_id: str
    status: JobStatus
    results: List[Dict]
    total_run_time: str

class ExtractionRequestModel(BaseModel):
    task_id: str
    pdf_key: str
    schemas: List[Dict]