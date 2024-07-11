from uuid import UUID, uuid4
from datetime import datetime
from typing import Dict, Union
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict

class PipelineConfig(BaseModel):
    model_config = ConfigDict()
    model_config['protected_namespaces'] = ()
    _id: UUID = PrivateAttr(default_factory=uuid4)
    name: str
    run_type: str
    pipeline_schema_id: UUID
    status: str = Field(default="active")
    created_by: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    integrations: Dict[str, str] = Field(default_factory=dict)
    model_id: str
    prompt_ids: Dict[str, str] = Field(default_factory=dict)
    normalization_id: str = Field(default="")

    def to_dict(self) -> Dict[str, Union[str, Dict[str, str], UUID]]:
        return {
            "id": str(self._id), 
            "name": self.name,
            "run_type": self.run_type, 
            "pipeline_schema_id": str(self.pipeline_schema_id), 
            "status": self.status,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "integrations": self.integrations,
            "model_id": self.model_id,
            "prompt_ids": self.prompt_ids,
            "normalization_id": self.normalization_id,
        }

    @staticmethod
    def from_dict(data: Dict[str, Union[str, Dict[str, str]]]) -> 'PipelineConfig':
        def parse_datetime(value):
            if isinstance(value, str):
                return datetime.fromisoformat(value)
            return datetime.now()

        instance = PipelineConfig(
            name=data.get("name", ""),
            run_type=data.get("run_type", ""),
            pipeline_schema_id=UUID(data.get("pipeline_schema_id", str(uuid4()))),
            status=data.get("status", "active"),
            created_by=data.get("created_by", ""),
            created_at=parse_datetime(data.get("created_at")),
            updated_at=parse_datetime(data.get("updated_at")),
            integrations=data.get("integrations", {}),
            model_id=data.get("model_id", ""),
            prompt_ids=data.get("prompt_ids", {}),
            normalization_id=data.get("normalization_id", ""),
        )
        instance._id = UUID(data.get("id", str(uuid4())))
        return instance

class PipelineConfigResponse(BaseModel):
    id: UUID
    name: str
    run_type: str
    pipeline_schema_id: UUID
    status: str
    created_by: str
    created_at: datetime
    updated_at: datetime
    integrations: Dict[str, str]
    model_id: str
    prompt_ids: Dict[str, str]
    normalization_id: str

    @staticmethod
    def from_pipeline_config(pipeline_config: PipelineConfig) -> 'PipelineConfigResponse':
        return PipelineConfigResponse(
            id=pipeline_config._id,
            name=pipeline_config.name,
            run_type=pipeline_config.run_type,
            pipeline_schema_id=pipeline_config.pipeline_schema_id,
            status=pipeline_config.status,
            created_by=pipeline_config.created_by,
            created_at=pipeline_config.created_at,
            updated_at=pipeline_config.updated_at,
            integrations=pipeline_config.integrations,
            model_id=pipeline_config.model_id,
            prompt_ids=pipeline_config.prompt_ids,
            normalization_id=pipeline_config.normalization_id,
        )
