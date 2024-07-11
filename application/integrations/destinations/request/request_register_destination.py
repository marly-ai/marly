from pydantic import BaseModel

class RegisterS3IntegrationRequest(BaseModel):
    aws_access_key: str
    aws_secret_key: str
    bucket_name: str
    region_name: str

class RegisterExcelIntegrationRequest(BaseModel):
    data_source: str
    output_filename: str

class RegisterPostgresIntegrationRequest(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = 'public'
