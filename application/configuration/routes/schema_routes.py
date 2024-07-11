from fastapi import APIRouter, HTTPException
from application.configuration.configs.schema_config import SchemaConfig, SchemaConfigResponse
from application.configuration.repositories.schema_repository import SchemaRepository
from uuid import UUID
from typing import List

api_router = APIRouter()

@api_router.post("/schemas", response_model=SchemaConfigResponse, status_code=201)
def add_schema(schema: SchemaConfig):
    try:
        SchemaRepository.add(schema)
        return SchemaConfigResponse.from_schema_config(schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/schemas/{schema_id}", response_model=SchemaConfigResponse, status_code=200)
def update_schema(schema_id: UUID, schema: SchemaConfig):
    try:
        SchemaRepository.update(schema_id, schema)
        return SchemaConfigResponse.from_schema_config(schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/schemas/{schema_id}", status_code=204)
def delete_schema(schema_id: UUID):
    try:
        SchemaRepository.delete(schema_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/schemas/{schema_id}", response_model=SchemaConfigResponse, status_code=200)
def get_schema(schema_id: UUID):
    try:
        schema = SchemaRepository.get(schema_id)
        if schema:
            return SchemaConfigResponse.from_schema_config(schema)
        else:
            raise HTTPException(status_code=404, detail="Schema not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/schemas", response_model=List[SchemaConfigResponse], status_code=200)
def list_schemas():
    try:
        schema_list = SchemaRepository.list()
        return [SchemaConfigResponse.from_schema_config(kw) for kw in schema_list]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))