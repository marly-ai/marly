from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from io import BytesIO

class BaseSource(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the data source."""
        pass

    @abstractmethod
    def read(self, data: Dict[str, Any]) -> Optional[BytesIO]:
        """Read a specific file from the data source."""
        pass

    @abstractmethod
    def read_all(self) -> List[str]:
        """Retrieve a list of all valid files in the data source."""
        pass