from common.sources.base_source import BaseSource
from typing import Dict, Optional, Any, List, Union
import boto3

class S3Integration(BaseSource):
    def __init__(self, aws_access_key: str, aws_secret_key: str, bucket_name: str, region_name: str) -> None:
        self.aws_access_key: str = aws_access_key
        self.aws_secret_key: str = aws_secret_key
        self.bucket_name: str = bucket_name
        self.region_name: str = region_name
        self.client: Optional[boto3.client] = None

    def connect(self) -> None:
        self.client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region_name
        )

    def read(self, data: Dict[str, Any]) -> Optional[Union[bytes, str]]:
        key: Optional[str] = data.get('file_key')
        if not key:
            raise ValueError("The 'file_key' must be provided in the data dictionary.")
        
        try:
            response: Dict[str, Any] = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except self.client.exceptions.NoSuchKey:
            print(f"File {key} not found in bucket {self.bucket_name}.")
            return None
    
    def read_all(self) -> List[str]:
        try:
            response: Dict[str, Any] = self.client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        except self.client.exceptions.NoSuchBucket:
            print(f"Bucket {self.bucket_name} not found.")
            return []