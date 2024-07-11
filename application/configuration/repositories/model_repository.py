from common.redis.redis_config import redis_client
from application.configuration.configs.model_config import ModelConfig
from uuid import UUID
from typing import List
import json

class ModelRepository:
    @staticmethod
    def add(model: ModelConfig):
        keys = redis_client.keys("model:*")
        for key in keys:
            existing_model_data = redis_client.get(key)
            if existing_model_data:
                existing_model = ModelConfig.from_dict(json.loads(existing_model_data))
                if existing_model.dict() == model.dict():
                    raise ValueError("Duplicate model found")
        redis_client.set(f"model:{model._id}", json.dumps(model.to_dict()))

    @staticmethod
    def update(model_id: UUID, model: ModelConfig):
        existing_model_data = redis_client.get(f"model:{model_id}")
        if not existing_model_data:
            raise ValueError("Model not found")

        existing_model = ModelConfig.from_dict(json.loads(existing_model_data))
        updated_data = existing_model.dict()
        updated_data.update(model.dict(exclude_unset=True))
        redis_client.set(f"model:{model_id}", json.dumps(updated_data))

    @staticmethod
    def delete(model_id: UUID):
        redis_client.delete(f"model:{model_id}")

    @staticmethod
    def get(model_id: UUID) -> ModelConfig:
        model_data = redis_client.get(f"model:{model_id}")
        return ModelConfig.from_dict(json.loads(model_data)) if model_data else None

    @staticmethod
    def list() -> List[ModelConfig]:
        keys = redis_client.keys("model:*")
        models = []
        for key in keys:
            model_data = redis_client.get(key)
            if model_data:
                model = ModelConfig.from_dict(json.loads(model_data))
                models.append(model)
        return models
