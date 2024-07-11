from common.redis.redis_config import redis_client
from application.configuration.configs.pipeline_config import PipelineConfig
from uuid import UUID
from typing import List
import json

class PipelineRepository:
    @staticmethod
    def add(pipeline: PipelineConfig):
        keys = redis_client.keys("pipeline:*")
        for key in keys:
            existing_pipeline_data = redis_client.get(key)
            if existing_pipeline_data:
                existing_pipeline = PipelineConfig.from_dict(json.loads(existing_pipeline_data))
                if existing_pipeline.dict() == pipeline.dict():
                    raise ValueError("Duplicate pipeline found")
        redis_client.set(f"pipeline:{pipeline._id}", json.dumps(pipeline.to_dict()))
    
    @staticmethod
    def update(pipeline_id: UUID, pipeline: PipelineConfig):
        existing_pipeline_data = redis_client.get(f"pipeline:{pipeline_id}")
        if not existing_pipeline_data:
            raise ValueError("Pipeline not found")

        existing_pipeline = PipelineConfig.from_dict(json.loads(existing_pipeline_data))
        
        # Update only the new fields
        updated_data = existing_pipeline.dict()
        updated_data.update(pipeline.dict(exclude_unset=True))
        
        # Ensure model_id is included in the updated data
        if pipeline.model_id:
            updated_data['model_id'] = str(pipeline.model_id)
        
        redis_client.set(f"pipeline:{pipeline_id}", json.dumps(updated_data))

    @staticmethod
    def get(pipeline_id: UUID) -> PipelineConfig:
        pipeline_data = redis_client.get(f"pipeline:{pipeline_id}")
        if pipeline_data:
            pipeline_dict = json.loads(pipeline_data)
            return PipelineConfig.from_dict(pipeline_dict)
        return None
    
    @staticmethod
    def delete(pipeline_id: UUID):
        redis_client.delete(f"pipeline:{pipeline_id}")

    @staticmethod
    def list() -> List[PipelineConfig]:
        keys = redis_client.keys("pipeline:*")
        pipelines = []
        for key in keys:
            pipeline_data = redis_client.get(key)
            if pipeline_data:
                pipeline = PipelineConfig.from_dict(json.loads(pipeline_data))
                pipelines.append(pipeline)
        return pipelines
