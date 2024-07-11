from uuid import UUID, uuid4
from datetime import datetime
from pydantic import BaseModel, Field, PrivateAttr
from typing import List, Dict, Optional, Any

class Message(BaseModel):
    role: str
    content: str

class PromptConfig(BaseModel):
    _id: UUID = PrivateAttr(default_factory=uuid4)
    messages: List[Message]
    name: Optional[str] = None
    type: Optional[str] = Field(default="")
    response_format: str = Field(default="")
    created_by: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    variables: Dict[str, Any] = Field(default_factory=dict)
    prompt_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self._id),
            "messages": [message.dict() for message in self.messages],
            "name": self.name,
            "type": self.type,
            "response_format": self.response_format,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "variables": self.variables,
            "prompt_id": self.prompt_id,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'PromptConfig':
        instance = PromptConfig(
            messages=[Message(**message) for message in data.get("messages", [])],
            name=data.get("name"),
            type=data.get("type", ""),
            response_format=data.get("response_format", ""),
            created_by=data.get("created_by", "unknown"),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.now(),
            variables=data.get("variables", {}),
            prompt_id=data.get("prompt_id"),
        )
        instance._id = UUID(data["id"]) if "id" in data else uuid4()
        return instance

class PromptConfigResponse(BaseModel):
    id: UUID
    messages: List[Message]
    name: Optional[str]
    type: Optional[str]
    response_format: str
    created_by: str
    created_at: datetime
    updated_at: datetime
    variables: Dict[str, Any]
    prompt_id: Optional[str]

    @staticmethod
    def from_prompt_config(prompt_config: PromptConfig) -> 'PromptConfigResponse':
        return PromptConfigResponse(
            id=prompt_config._id,
            messages=prompt_config.messages,
            name=prompt_config.name,
            type=prompt_config.type,
            response_format=prompt_config.response_format,
            created_by=prompt_config.created_by,
            created_at=prompt_config.created_at,
            updated_at=prompt_config.updated_at,
            variables=prompt_config.variables,
            prompt_id=prompt_config.prompt_id,
        )