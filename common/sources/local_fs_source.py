from common.sources.base.base_source import BaseSource
from typing import Dict, Optional, Any, List, Union
import os
from io import BytesIO
import logging
import mimetypes

logger = logging.getLogger(__name__)

class LocalFSIntegration(BaseSource):
    ALLOWED_EXTENSIONS = {'.pdf', '.pptx', '.docx'}
    ALLOWED_MIMETYPES = {
        'application/pdf',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    }

    def __init__(self, base_path: str, additional_params: Optional[Dict[str, Any]] = None) -> None:
        self.base_path: str = base_path
        self.additional_params = additional_params or {}
        self.connect()

    def connect(self) -> None:
        if not os.path.exists(self.base_path):
            raise ValueError(f"The specified base path does not exist: {self.base_path}")

    @staticmethod
    def is_valid_file(file_path: str) -> bool:
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in LocalFSIntegration.ALLOWED_EXTENSIONS:
            return False
        
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type in LocalFSIntegration.ALLOWED_MIMETYPES

    def read(self, data: Dict[str, Any]) -> Optional[BytesIO]:
        file_key: Optional[str] = data.get('file_key')
        if not file_key:
            raise ValueError("The 'file_key' must be provided in the data dictionary.")
        
        file_path = os.path.join(self.base_path, file_key)
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} not found.")
            return None

        if not self.is_valid_file(file_path):
            logger.warning(f"File {file_path} is not a valid PDF, PowerPoint, or Word document.")
            return None

        try:
            with open(file_path, 'rb') as file:
                return BytesIO(file.read())
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return None

    def read_all(self) -> List[str]:
        """Retrieve a list of all valid files in the base directory."""
        try:
            files = os.listdir(self.base_path)
            valid_files = [
                f for f in files
                if self.is_valid_file(os.path.join(self.base_path, f))
            ]
            logger.info(f"Retrieved {len(valid_files)} valid files from {self.base_path}")
            return valid_files
        except Exception as e:
            logger.error(f"Error listing files in {self.base_path}: {e}")
            return []