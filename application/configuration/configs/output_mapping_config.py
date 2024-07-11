from typing import Dict, Optional, List
from pydantic import BaseModel, Field, PrivateAttr
from uuid import UUID, uuid4
from datetime import datetime

class OutputMappingConfig(BaseModel):
    _id: UUID = PrivateAttr(default_factory=uuid4)
    output_filename: Optional[str] = None
    output_data_type: Optional[str] = None
    status: str = Field(default="active")
    created_by: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    column_locations: Optional[Dict[str, Dict[str, str]]] = Field(default_factory=dict)
    data_locations: List[str] = Field(default_factory=list)

    def to_dict(self) -> Dict[str, str]:
        return {
            "id": str(self._id),
            "output_filename": self.output_filename,
            "output_data_type": self.output_data_type,
            "status": self.status,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "column_locations": self.column_locations if self.column_locations else {},
            "data_locations": self.data_locations if self.data_locations else []
        }

    @staticmethod
    def from_config(config: Dict[str, str]) -> 'OutputMappingConfig':
        try:
            instance = OutputMappingConfig(
                output_filename=config.get("output_filename"),
                output_data_type=config.get("output_data_type"),
                status=config.get("status", "active"),
                created_by=config.get("created_by", "unknown"),
                created_at=datetime.fromisoformat(config["created_at"]) if config.get("created_at") else datetime.now(),
                updated_at=datetime.fromisoformat(config["updated_at"]) if config.get("updated_at") else datetime.now(),
                column_locations=config.get("column_locations", {}),
                data_locations=config.get("data_locations", [])
            )
            instance._id = UUID(config["id"]) if "id" in config else uuid4()
            return instance
        except KeyError as e:
            raise ValueError(f"Missing required field: {e}")
        except Exception as e:
            raise ValueError(f"Error parsing config: {e}")

    @staticmethod
    def from_dict(data: Dict[str, any]) -> 'OutputMappingConfig':
        instance = OutputMappingConfig(
            output_filename=data.get("output_filename"),
            output_data_type=data.get("output_data_type"),
            status=data.get("status", "active"),
            created_by=data.get("created_by", "unknown"),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.now(),
            column_locations=data.get("column_locations", {}),
            data_locations=data.get("data_locations", [])
        )
        instance._id = UUID(data["id"]) if "id" in data else uuid4()
        return instance

class OutputMappingConfigResponse(BaseModel):
    id: UUID
    output_filename: Optional[str]
    output_data_type: Optional[str]
    status: str
    created_by: str
    created_at: datetime
    updated_at: datetime
    column_locations: Optional[Dict[str, Dict[str, str]]]
    data_locations: List[str]

    @staticmethod
    def from_output_mapping_config(output_mapping_config: OutputMappingConfig) -> 'OutputMappingConfigResponse':
        return OutputMappingConfigResponse(
            id=output_mapping_config._id,
            output_filename=output_mapping_config.output_filename,
            output_data_type=output_mapping_config.output_data_type,
            status=output_mapping_config.status,
            created_by=output_mapping_config.created_by,
            created_at=output_mapping_config.created_at,
            updated_at=output_mapping_config.updated_at,
            column_locations=output_mapping_config.column_locations,
            data_locations=output_mapping_config.data_locations
        )
