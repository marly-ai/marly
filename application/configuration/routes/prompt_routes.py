from fastapi import APIRouter, HTTPException
from application.configuration.configs.prompt_config import PromptConfig, PromptConfigResponse
from application.configuration.repositories.prompt_repository import PromptRepository
from uuid import UUID
from typing import List

api_router = APIRouter()

@api_router.post("/prompts", response_model=PromptConfigResponse, status_code=201)
def add_prompt(prompt: PromptConfig):
    try:
        PromptRepository.add(prompt)
        return PromptConfigResponse.from_prompt_config(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/prompts/{prompt_id}", response_model=PromptConfigResponse, status_code=200)
def update_prompt(prompt_id: UUID, prompt: PromptConfig):
    try:
        PromptRepository.update(prompt_id, prompt)
        return PromptConfigResponse.from_prompt_config(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/prompts/{prompt_id}", status_code=204)
def delete_prompt(prompt_id: UUID):
    try:
        PromptRepository.delete(prompt_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/prompts/{prompt_id}", response_model=PromptConfigResponse, status_code=200)
def get_prompt(prompt_id: UUID):
    try:
        prompt = PromptRepository.get(prompt_id)
        if prompt:
            return PromptConfigResponse.from_prompt_config(prompt)
        else:
            raise HTTPException(status_code=404, detail="Prompt not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/prompts", response_model=List[PromptConfigResponse], status_code=200)
def list_prompts():
    try:
        prompts = PromptRepository.list()
        return [PromptConfigResponse.from_prompt_config(prompt) for prompt in prompts]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
