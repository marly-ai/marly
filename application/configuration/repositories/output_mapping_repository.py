from common.redis.redis_config import redis_client
from application.configuration.configs.output_mapping_config import OutputMappingConfig
from uuid import UUID
from typing import List
import json

class OutputMappingRepository:
    @staticmethod
    def add(output_mapping: OutputMappingConfig):
        keys = redis_client.keys("output_mapping:*")
        for key in keys:
            existing_output_mapping_data = redis_client.get(key)
            if existing_output_mapping_data:
                existing_output_mapping = OutputMappingConfig.from_dict(json.loads(existing_output_mapping_data))
                if existing_output_mapping.dict() == output_mapping.dict():
                    raise ValueError("Duplicate output mapping found")
        redis_client.set(f"output_mapping:{output_mapping._id}", json.dumps(output_mapping.to_dict()))

    @staticmethod
    def update(output_mapping_id: UUID, output_mapping: OutputMappingConfig):
        existing_output_mapping_data = redis_client.get(f"output_mapping:{output_mapping_id}")
        if not existing_output_mapping_data:
            raise ValueError("Output mapping not found")

        existing_output_mapping = OutputMappingConfig.from_dict(json.loads(existing_output_mapping_data))
        updated_data = existing_output_mapping.dict()
        updated_data.update(output_mapping.dict(exclude_unset=True))
        redis_client.set(f"output_mapping:{output_mapping_id}", json.dumps(updated_data))

    @staticmethod
    def delete(output_mapping_id: UUID):
        redis_client.delete(f"output_mapping:{output_mapping_id}")

    @staticmethod
    def get(output_mapping_id: UUID) -> OutputMappingConfig:
        output_mapping_data = redis_client.get(f"output_mapping:{output_mapping_id}")
        return OutputMappingConfig.from_dict(json.loads(output_mapping_data)) if output_mapping_data else None

    @staticmethod
    def list() -> List[OutputMappingConfig]:
        keys = redis_client.keys("output_mapping:*")
        output_mappings = []
        for key in keys:
            output_mapping_data = redis_client.get(key)
            if output_mapping_data:
                output_mapping = OutputMappingConfig.from_dict(json.loads(output_mapping_data))
                output_mappings.append(output_mapping)
        return output_mappings

    @staticmethod
    def get_by_filename(filename: str) -> OutputMappingConfig:
        keys = redis_client.keys("output_mapping:*")
        for key in keys:
            output_mapping_data = redis_client.get(key)
            if output_mapping_data:
                output_mapping = OutputMappingConfig.from_dict(json.loads(output_mapping_data))
                if output_mapping.output_filename == filename:
                    return output_mapping
        return None