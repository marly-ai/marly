from uuid import UUID, uuid4
from datetime import datetime
from pydantic import BaseModel, Field, PrivateAttr, validator
from typing import Dict, Optional
import importlib
import os

class NormalizationConfig(BaseModel):
    model_config = {"protected_namespaces": ()}
    _id: UUID = PrivateAttr(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    normalization_type: str
    output_data_source: str
    output_filename: str
    additional_params: Dict[str, Optional[str]] = Field(default_factory=dict)
    created_by: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    @staticmethod
    def get_allowed_normalizations():
        try:
            normalizations_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "common", "normalization")
            return [f.split('_')[0] for f in os.listdir(normalizations_dir) if f.endswith('_normalization.py')]
        except FileNotFoundError:
            print(f"Warning: Normalizations directory not found at {normalizations_dir}")
            return []

    @validator('normalization_type')
    def validate_normalization_type(cls, v):
        allowed_normalizations = cls.get_allowed_normalizations()
        if v not in allowed_normalizations:
            raise ValueError(f"Normalization type '{v}' is not allowed. Allowed normalizations are: {allowed_normalizations}")
        return v

    @staticmethod
    def get_normalization_instance(normalization_config):
        try:
            module_name = f"common.normalization.{normalization_config.normalization_type}_normalization"
            module = importlib.import_module(module_name)
            normalization_class = getattr(module, normalization_config.normalization_type.capitalize() + "Normalization")
            return normalization_class(normalization_config.output_data_source, normalization_config.output_filename)
        except (ModuleNotFoundError, AttributeError) as e:
            print(f"Error instantiating normalization class: {e}")
            raise

    def to_dict(self) -> Dict[str, any]:
        return {
            "id": str(self._id),
            "name": self.name,
            "description": self.description,
            "normalization_type": self.normalization_type,
            "output_data_source": self.output_data_source,
            "output_filename": self.output_filename,
            "additional_params": self.additional_params,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    @staticmethod
    def from_dict(data: Dict[str, any]) -> 'NormalizationConfig':
        instance = NormalizationConfig(
            name=data.get("name", ""),
            description=data.get("description"),
            normalization_type=data.get("normalization_type", ""),
            output_data_source=data.get("output_data_source", ""),
            output_filename=data.get("output_filename", ""),
            additional_params=data.get("additional_params", {}),
            created_by=data.get("created_by", "unknown"),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.now(),
        )
        instance._id = UUID(data["id"]) if "id" in data else uuid4()
        return instance

class NormalizationConfigResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str]
    normalization_type: str
    output_data_source: str
    output_filename: str
    additional_params: Dict[str, Optional[str]]
    created_by: str
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def from_normalization_config(normalization_config: NormalizationConfig) -> 'NormalizationConfigResponse':
        return NormalizationConfigResponse(
            id=normalization_config._id,
            name=normalization_config.name,
            description=normalization_config.description,
            normalization_type=normalization_config.normalization_type,
            output_data_source=normalization_config.output_data_source,
            output_filename=normalization_config.output_filename,
            additional_params=normalization_config.additional_params,
            created_by=normalization_config.created_by,
            created_at=normalization_config.created_at,
            updated_at=normalization_config.updated_at
        )
