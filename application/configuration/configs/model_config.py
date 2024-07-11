from uuid import UUID, uuid4
from datetime import datetime
from pydantic import BaseModel, Field, PrivateAttr, validator
from typing import Dict, Optional
import importlib
import os

class ModelConfig(BaseModel):
    model_config = {"protected_namespaces": ()}
    _id: UUID = PrivateAttr(default_factory=uuid4)
    api_key: str
    model_type: str
    model_name: str
    additional_params: Dict[str, Optional[str]] = Field(default_factory=dict)
    created_by: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    @staticmethod
    def get_allowed_models():
        try:
            models_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "common", "models")
            return [f.split('_')[0] for f in os.listdir(models_dir) if f.endswith('_model.py')]
        except FileNotFoundError:
            print(f"Warning: Models directory not found at {models_dir}")
            return []

    @validator('model_type')
    def validate_model_type(cls, v):
        allowed_models = cls.get_allowed_models()
        if v not in allowed_models:
            raise ValueError(f"Model type '{v}' is not allowed. Allowed models are: {allowed_models}")
        return v

    @staticmethod
    def get_model_instance(model_config):
        try:
            module_name = f"common.models.{model_config.model_type}_model"
            module = importlib.import_module(module_name)
            model_class = getattr(module, model_config.model_type.capitalize() + "Model")
            return model_class(model_config)
        except (ModuleNotFoundError, AttributeError) as e:
            print(f"Error instantiating model class: {e}")
            raise

    def to_dict(self) -> Dict[str, any]:
        return {
            "id": str(self._id),
            "model_name": self.model_name,
            "api_key": self.api_key,
            "model_type": self.model_type,
            "additional_params": self.additional_params,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    @staticmethod
    def from_dict(data: Dict[str, any]) -> 'ModelConfig':
        instance = ModelConfig(
            model_name=data.get("model_name", ""),
            api_key=data.get("api_key", ""),
            model_type=data.get("model_type", ""),
            additional_params=data.get("additional_params", {}),
            created_by=data.get("created_by", "unknown"),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.now(),
        )
        instance._id = UUID(data["id"]) if "id" in data else uuid4()
        return instance

class ModelConfigResponse(BaseModel):
    id: UUID
    model_name: str
    model_type: str
    additional_params: Dict[str, Optional[str]]
    created_by: str
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def from_model_config(model_config: ModelConfig) -> 'ModelConfigResponse':
        return ModelConfigResponse(
            id=model_config._id,
            model_name=model_config.model_name,
            model_type=model_config.model_type,
            additional_params=model_config.additional_params,
            created_by=model_config.created_by,
            created_at=model_config.created_at,
            updated_at=model_config.updated_at
        )