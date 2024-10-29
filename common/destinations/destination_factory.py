from typing import Dict, Any
from common.destinations.enums.destination_enums import DestinationType
from common.destinations.sqlite_destination import SQLiteDestination

class DestinationFactory:
    @staticmethod
    def create_destination(destination_type: str, config: Dict[str, Any] = None):
        if not destination_type:
            raise ValueError("destination_type must be provided.")

        try:
            destination_type_enum = DestinationType(destination_type.lower())
        except ValueError:
            raise ValueError(f"Invalid destination type. Allowed values are: {', '.join([d.value for d in DestinationType])}")

        if destination_type_enum == DestinationType.SQLITE:
            return SQLiteDestination(
                db_path=config.get("db_path", ":memory:"),
                additional_params=config.get("additional_params", {})
            )
        else:
            raise ValueError(f"Unsupported destination type: {destination_type_enum}")

