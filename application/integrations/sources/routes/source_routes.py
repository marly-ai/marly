from fastapi import APIRouter
from application.integrations.sources.request.register_s3_connector import RegisterS3SourceRequest
from application.integrations.sources.response.register_connector_response import RegisterSourceResponse
from application.integrations.sources.repositories.source_repository import SourceRepository
from common.sources.s3_source import S3Integration
from fastapi import HTTPException
from application.integrations.sources.request.register_local_fs_connector import RegisterLocalFSSourceRequest
from common.sources.local_fs_source import LocalFSIntegration
import os

api_router = APIRouter()

@api_router.post("/register-s3", response_model=RegisterSourceResponse)
def register_s3_integration(request: RegisterS3SourceRequest):
    config = {
        'aws_access_key': request.aws_access_key,
        'aws_secret_key': request.aws_secret_key,
        'bucket_name': request.bucket_name,
        'region_name': request.region_name
    }
    SourceRepository.register_integration('s3_source', config, S3Integration)
    return RegisterSourceResponse(success=True, message="S3 Integration registered successfully.")

@api_router.post("/register-local-fs", response_model=RegisterSourceResponse)
def register_local_fs_integration(request: RegisterLocalFSSourceRequest):
    if not os.path.exists(request.base_path):
        raise HTTPException(status_code=400, detail="The specified base path does not exist.")
    
    valid_files = [f for f in os.listdir(request.base_path) if os.path.isfile(os.path.join(request.base_path, f)) and LocalFSIntegration.is_valid_file(os.path.join(request.base_path, f))]
    
    if not valid_files:
        raise HTTPException(status_code=400, detail="The specified directory does not contain any valid PDF, PowerPoint, or Word documents.")

    config = {
        'base_path': request.base_path
    }
    SourceRepository.register_integration('local_fs_source', config, LocalFSIntegration)
    return RegisterSourceResponse(success=True, message="Local File System Integration registered successfully.")
