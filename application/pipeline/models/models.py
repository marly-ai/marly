from pydantic import BaseModel, Field
from typing import List, Dict, Any
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class WorkloadItem(BaseModel):
    pdf_stream: str
    schemas: List[str]

class PipelineRequestModel(BaseModel):
    workloads: List[WorkloadItem]
    provider_type: str
    provider_model_name: str
    api_key: str
    additional_params: Dict[str, Any] = Field(default_factory=dict)
    markdown_mode: bool = False
    additional_params: Dict[str, Any] = {}

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
