from pydantic import BaseModel
from typing import Optional

class PipelineResponseModel(BaseModel):
    job_status: str
    total_runtime: str
    output_destination: str
    output_filename: Optional[str] = None