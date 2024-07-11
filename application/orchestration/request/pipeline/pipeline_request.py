from pydantic import BaseModel
from typing import List
from uuid import UUID

class PipelineRequestModel(BaseModel):
    filenames: List[str]
    pipeline_id: UUID
    output_mapping_ids: List[UUID]
