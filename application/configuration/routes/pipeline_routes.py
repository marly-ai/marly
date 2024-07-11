from fastapi import APIRouter, HTTPException
from application.configuration.configs.pipeline_config import PipelineConfig, PipelineConfigResponse
from application.configuration.repositories.pipeline_respository import PipelineRepository
from uuid import UUID
from typing import List

api_router = APIRouter()

@api_router.post("/pipelines", response_model=PipelineConfigResponse, status_code=201)
def add_pipeline(pipeline: PipelineConfig):
    try:
        PipelineRepository.add(pipeline)
        return PipelineConfigResponse.from_pipeline_config(pipeline)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/pipelines/{pipeline_id}", response_model=PipelineConfigResponse, status_code=200)
def update_pipeline(pipeline_id: UUID, pipeline: PipelineConfig):
    try:
        PipelineRepository.update(pipeline_id, pipeline)
        return PipelineConfigResponse.from_pipeline_config(pipeline)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/pipelines/{pipeline_id}", status_code=204)
def delete_pipeline(pipeline_id: UUID):
    try:
        PipelineRepository.delete(pipeline_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/pipelines/{pipeline_id}", response_model=PipelineConfigResponse, status_code=200)
def get_pipeline(pipeline_id: UUID):
    try:
        pipeline = PipelineRepository.get(pipeline_id)
        if pipeline:
            return PipelineConfigResponse.from_pipeline_config(pipeline)
        else:
            raise HTTPException(status_code=404, detail="Pipeline not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/pipelines", response_model=List[PipelineConfigResponse], status_code=200)
def list_pipelines():
    try:
        pipelines = PipelineRepository.list()
        return [PipelineConfigResponse.from_pipeline_config(p) for p in pipelines]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))