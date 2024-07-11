from pydantic import BaseModel

class RegisterS3SourceRequest(BaseModel):
    aws_access_key: str
    aws_secret_key: str
    bucket_name: str
    region_name: str
