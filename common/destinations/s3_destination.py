from common.destinations.base_destination import BaseDestination
from typing import Dict, Optional
from boto3.s3.transfer import TransferConfig
from io import BytesIO
import boto3
import logging

logger = logging.getLogger(__name__)

class S3Integration(BaseDestination):
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
    
    def read(self, data: Dict):
        key = data.get('file_key')
        if not key:
            raise ValueError("The 'file_key' must be provided in the data dictionary.")
        
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return BytesIO(response['Body'].read())
        except self.client.exceptions.NoSuchKey:
            print(f"File {key} not found in bucket {self.bucket_name}.")
            return None

    def write(self, data: Dict):
        try:
            file_name = data.get('file_name')
            file_stream = data.get('file_stream')
            if not file_name or not file_stream:
                raise ValueError("Both 'file_name' and 'file_stream' must be provided in the data dictionary.")
            
            logger.info(f"Preparing to upload file to S3. Bucket: {self.bucket_name}, File Name: {file_name}")

            config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 25, use_threads=True)
            
            logger.info(f"TransferConfig set with multipart_threshold: {config.multipart_threshold}, max_concurrency: {config.max_concurrency}, multipart_chunksize: {config.multipart_chunksize}, use_threads: {config.use_threads}")

            if isinstance(file_stream, BytesIO):
                file_stream.seek(0)
            else:
                file_stream = BytesIO(file_stream)

            self.client.upload_fileobj(
                file_stream,
                self.bucket_name,
                file_name,
                Config=config
            )
            
            logger.info(f"File {file_name} successfully uploaded to bucket {self.bucket_name}")
            return file_name
        except Exception as e:
            logger.error(f"Error writing to S3: {e}", exc_info=True)
            raise    
    
