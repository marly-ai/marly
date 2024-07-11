from typing import Dict
from pydantic import BaseModel, Field, PrivateAttr
from uuid import UUID, uuid4
from datetime import datetime

class SchemaConfig(BaseModel):
    _id: UUID = PrivateAttr(default_factory=uuid4) 
    keywords: Dict[str, str] = Field(default_factory=dict)
    status: str = Field(default="active")
    created_by: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, any]:
        return {
            "id": str(self._id),
            "keywords": self.keywords,
            "status": self.status,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    @staticmethod
    def from_config(config: Dict[str, any]) -> 'SchemaConfig':
        try:
            instance = SchemaConfig(
                keywords=config.get("keywords", {}),
                status=config.get("status", "active"),
                created_by=config.get("created_by", "unknown"),
                created_at=datetime.fromisoformat(config["created_at"]) if "created_at" in config else datetime.now(),
                updated_at=datetime.fromisoformat(config["updated_at"]) if "updated_at" in config else datetime.now(),
            )
            instance._id = UUID(config["id"]) if "id" in config else uuid4()
            return instance
        except KeyError as e:
            raise ValueError(f"Missing required field: {e}")
        except Exception as e:
            raise ValueError(f"Error parsing config: {e}")

class SchemaConfigResponse(BaseModel):
    id: UUID
    keywords: Dict[str, str]
    status: str
    created_by: str
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def from_schema_config(scheme_config: SchemaConfig) -> 'SchemaConfigResponse':
        return SchemaConfigResponse(
            id=scheme_config._id,
            keywords=scheme_config.keywords,
            status=scheme_config.status,
            created_by=scheme_config.created_by,
            created_at=scheme_config.created_at,
            updated_at=scheme_config.updated_at
        )
