from common.redis.redis_config import redis_client
from application.configuration.configs.schema_config import SchemaConfig
from uuid import UUID
from typing import List
import json

class SchemaRepository:
    @staticmethod
    def add(schema: SchemaConfig):
        keys = redis_client.keys("schema:*")
        for key in keys:
            existing_schema_data = redis_client.get(key)
            if existing_schema_data:
                existing_schema = SchemaConfig.from_config(json.loads(existing_schema_data))
                if existing_schema.dict() == schema.dict():
                    raise ValueError("Duplicate schema found")
        redis_client.set(f"schema:{schema._id}", json.dumps(schema.to_dict()))

    @staticmethod
    def update(schema_id: UUID, schema: SchemaConfig):
        existing_schema_data = redis_client.get(f"schema:{schema_id}")
        if not existing_schema_data:
            raise ValueError("Schema not found")

        existing_schema = SchemaConfig.from_config(json.loads(existing_schema_data))
        updated_data = existing_schema.dict()
        updated_data.update(schema.dict(exclude_unset=True))
        redis_client.set(f"schema:{schema_id}", json.dumps(updated_data))

    @staticmethod
    def delete(schema_id: UUID):
        redis_client.delete(f"schema:{schema_id}")

    @staticmethod
    def get(schema_id: UUID) -> SchemaConfig:
        schema_data = redis_client.get(f"schema:{schema_id}")
        return SchemaConfig.from_config(json.loads(schema_data)) if schema_data else None

    @staticmethod
    def list() -> List[SchemaConfig]:
        keys = redis_client.keys("schema:*")
        schema_list = []
        for key in keys:
            schema_data = redis_client.get(key)
            if schema_data:
                schema = SchemaConfig.from_config(json.loads(schema_data))
                schema_list.append(schema)
        return schema_list
