from common.sources.base.base_source import BaseSource
from typing import Dict, Optional, Any, List
from io import BytesIO
import boto3
import os
import logging

logger = logging.getLogger(__name__)

class S3Integration(BaseSource):
    def __init__(self, bucket_name: str, additional_params: Optional[Dict[str, Any]] = None) -> None:
        self.bucket_name = bucket_name
        self.additional_params = additional_params or {}
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.additional_params.get('aws_access_key_id'),
            aws_secret_access_key=self.additional_params.get('aws_secret_access_key'),
            aws_session_token=self.additional_params.get('aws_session_token')
        )
        self.connect()

    def connect(self) -> None:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket {self.bucket_name}: {e}")
            raise

    def read(self, data: Dict[str, Any]) -> Optional[BytesIO]:
        file_key: Optional[str] = data.get('file_key')
        if not file_key:
            raise ValueError("The 'file_key' must be provided in the data dictionary.")
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            return BytesIO(response['Body'].read())
        except Exception as e:
            logger.error(f"Error reading file from S3: {e}")
            return None

    def read_all(self) -> List[str]:
        """Retrieve a list of all valid files in the S3 bucket."""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name)
            valid_files = []
            for page in page_iterator:
                contents = page.get('Contents', [])
                for obj in contents:
                    key = obj['Key']
                    if self.is_valid_file(key):
                        valid_files.append(key)
            logger.info(f"Retrieved {len(valid_files)} valid files from S3 bucket {self.bucket_name}")
            return valid_files
        except Exception as e:
            logger.error(f"Error listing files in S3 bucket {self.bucket_name}: {e}")
            return []

    @staticmethod
    def is_valid_file(file_key: str) -> bool:
        allowed_extensions = {'.pdf', '.pptx', '.docx'}
        _, ext = os.path.splitext(file_key)
        return ext.lower() in allowed_extensions