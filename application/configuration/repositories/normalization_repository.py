from common.redis.redis_config import redis_client
from application.configuration.configs.normalization_config import NormalizationConfig
from uuid import UUID
from typing import List
import json

class NormalizationRepository:
    @staticmethod
    def add(normalization: NormalizationConfig):
        keys = redis_client.keys("normalization:*")
        for key in keys:
            existing_normalization_data = redis_client.get(key)
            if existing_normalization_data:
                existing_normalization = NormalizationConfig.from_dict(json.loads(existing_normalization_data))
                if existing_normalization.dict() == normalization.dict():
                    raise ValueError("Duplicate normalization found")
        redis_client.set(f"normalization:{normalization._id}", json.dumps(normalization.to_dict()))

    @staticmethod
    def update(normalization_id: UUID, normalization: NormalizationConfig):
        existing_normalization_data = redis_client.get(f"normalization:{normalization_id}")
        if not existing_normalization_data:
            raise ValueError("Normalization not found")

        existing_normalization = NormalizationConfig.from_dict(json.loads(existing_normalization_data))
        updated_data = existing_normalization.dict()
        updated_data.update(normalization.dict(exclude_unset=True))
        redis_client.set(f"normalization:{normalization_id}", json.dumps(updated_data))

    @staticmethod
    def delete(normalization_id: UUID):
        redis_client.delete(f"normalization:{normalization_id}")

    @staticmethod
    def get(normalization_id: UUID) -> NormalizationConfig:
        normalization_data = redis_client.get(f"normalization:{normalization_id}")
        return NormalizationConfig.from_dict(json.loads(normalization_data)) if normalization_data else None

    @staticmethod
    def list() -> List[NormalizationConfig]:
        keys = redis_client.keys("normalization:*")
        normalizations = []
        for key in keys:
            normalization_data = redis_client.get(key)
            if normalization_data:
                normalization = NormalizationConfig.from_dict(json.loads(normalization_data))
                normalizations.append(normalization)
        return normalizations