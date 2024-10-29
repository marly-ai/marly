from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class BaseDestination(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the destination."""
        pass

    @abstractmethod
    def insert(self, data: List[Dict[str, Any]]) -> None:
        """Insert data into the destination."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the destination."""
        pass

    @abstractmethod
    def get_table_structure(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get the structure of a table."""
        pass
