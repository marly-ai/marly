from fastapi import APIRouter, HTTPException
from application.configuration.configs.normalization_config import NormalizationConfig, NormalizationConfigResponse
from application.configuration.repositories.normalization_repository import NormalizationRepository
from uuid import UUID
from typing import List

api_router = APIRouter()

@api_router.post("/normalizations", response_model=NormalizationConfigResponse, status_code=201)
def add_normalization(normalization: NormalizationConfig):
    try:
        NormalizationRepository.add(normalization)
        return NormalizationConfigResponse.from_normalization_config(normalization)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/normalizations/{normalization_id}", response_model=NormalizationConfigResponse, status_code=200)
def update_normalization(normalization_id: UUID, normalization: NormalizationConfig):
    try:
        NormalizationRepository.update(normalization_id, normalization)
        return NormalizationConfigResponse.from_normalization_config(normalization)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/normalizations/{normalization_id}", status_code=204)
def delete_normalization(normalization_id: UUID):
    try:
        NormalizationRepository.delete(normalization_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/normalizations/{normalization_id}", response_model=NormalizationConfigResponse, status_code=200)
def get_normalization(normalization_id: UUID):
    try:
        normalization = NormalizationRepository.get(normalization_id)
        if normalization:
            return NormalizationConfigResponse.from_normalization_config(normalization)
        else:
            raise HTTPException(status_code=404, detail="Normalization not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/normalizations", response_model=List[NormalizationConfigResponse], status_code=200)
def list_normalizations():
    try:
        normalizations = NormalizationRepository.list()
        return [NormalizationConfigResponse.from_normalization_config(n) for n in normalizations]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
