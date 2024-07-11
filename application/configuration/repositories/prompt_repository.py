from common.redis.redis_config import redis_client
from application.configuration.configs.prompt_config import PromptConfig
from uuid import UUID
from typing import List
import json

class PromptRepository:
    @staticmethod
    def add(prompt: PromptConfig):
        keys = redis_client.keys("prompt:*")
        for key in keys:
            existing_prompt_data = redis_client.get(key)
            if existing_prompt_data:
                existing_prompt = PromptConfig.from_dict(json.loads(existing_prompt_data))
                if existing_prompt.dict() == prompt.dict():
                    raise ValueError("Duplicate prompt found")
        redis_client.set(f"prompt:{prompt._id}", json.dumps(prompt.to_dict()))

    @staticmethod
    def update(prompt_id: UUID, prompt: PromptConfig):
        existing_prompt_data = redis_client.get(f"prompt:{prompt_id}")
        if not existing_prompt_data:
            raise ValueError("Prompt not found")

        existing_prompt = PromptConfig.from_dict(json.loads(existing_prompt_data))
        updated_data = existing_prompt.dict()
        updated_data.update(prompt.dict(exclude_unset=True))
        redis_client.set(f"prompt:{prompt_id}", json.dumps(updated_data))

    @staticmethod
    def delete(prompt_id: UUID):
        redis_client.delete(f"prompt:{prompt_id}")

    @staticmethod
    def get(prompt_id: UUID) -> PromptConfig:
        prompt_data = redis_client.get(f"prompt:{prompt_id}")
        return PromptConfig.from_dict(json.loads(prompt_data)) if prompt_data else None

    @staticmethod
    def list() -> List[PromptConfig]:
        keys = redis_client.keys("prompt:*")
        prompts = []
        for key in keys:
            prompt_data = redis_client.get(key)
            if prompt_data:
                prompt = PromptConfig.from_dict(json.loads(prompt_data))
                prompts.append(prompt)
        return prompts
