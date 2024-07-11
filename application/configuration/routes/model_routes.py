from fastapi import APIRouter, HTTPException
from application.configuration.configs.model_config import ModelConfig, ModelConfigResponse
from application.configuration.repositories.model_repository import ModelRepository
from uuid import UUID
from typing import List

api_router = APIRouter()

@api_router.post("/models", response_model=ModelConfigResponse, status_code=201)
def add_model(model: ModelConfig):
    try:
        ModelRepository.add(model)
        return ModelConfigResponse.from_model_config(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/models/{model_id}", response_model=ModelConfigResponse, status_code=200)
def update_model(model_id: UUID, model: ModelConfig):
    try:
        ModelRepository.update(model_id, model)
        return ModelConfigResponse.from_model_config(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/models/{model_id}", status_code=204)
def delete_model(model_id: UUID):
    try:
        ModelRepository.delete(model_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/models/{model_id}", response_model=ModelConfigResponse, status_code=200)
def get_model(model_id: UUID):
    try:
        model = ModelRepository.get(model_id)
        if model:
            return ModelConfigResponse.from_model_config(model)
        else:
            raise HTTPException(status_code=404, detail="Model not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/models", response_model=List[ModelConfigResponse], status_code=200)
def list_models():
    try:
        models = ModelRepository.list()
        return [ModelConfigResponse.from_model_config(model) for model in models]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
