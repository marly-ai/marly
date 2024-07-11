from fastapi import APIRouter, HTTPException
from application.configuration.configs.output_mapping_config import OutputMappingConfig, OutputMappingConfigResponse
from application.configuration.repositories.output_mapping_repository import OutputMappingRepository
from uuid import UUID
from typing import List

api_router = APIRouter()

@api_router.post("/output-mappings", response_model=OutputMappingConfigResponse, status_code=201)
def add_output_mapping(output_mapping: OutputMappingConfig):
    try:
        OutputMappingRepository.add(output_mapping)
        return OutputMappingConfigResponse.from_output_mapping_config(output_mapping)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/output-mappings/{output_mapping_id}", response_model=OutputMappingConfigResponse, status_code=200)
def update_output_mapping(output_mapping_id: UUID, output_mapping: OutputMappingConfig):
    try:
        OutputMappingRepository.update(output_mapping_id, output_mapping)
        return OutputMappingConfigResponse.from_output_mapping_config(output_mapping)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/output-mappings/{output_mapping_id}", status_code=204)
def delete_output_mapping(output_mapping_id: UUID):
    try:
        OutputMappingRepository.delete(output_mapping_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/output-mappings/{output_mapping_id}", response_model=OutputMappingConfigResponse, status_code=200)
def get_output_mapping(output_mapping_id: UUID):
    try:
        output_mapping = OutputMappingRepository.get(output_mapping_id)
        if output_mapping:
            return OutputMappingConfigResponse.from_output_mapping_config(output_mapping)
        else:
            raise HTTPException(status_code=404, detail="Output mapping not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/output-mappings", response_model=List[OutputMappingConfigResponse], status_code=200)
def list_output_mappings():
    try:
        output_mappings = OutputMappingRepository.list()
        return [OutputMappingConfigResponse.from_output_mapping_config(mapping) for mapping in output_mappings]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
