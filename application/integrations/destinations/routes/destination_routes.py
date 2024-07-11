from application.integrations.destinations.repositories.destination_repository import DestinationRepository
from common.destinations.s3_destination import S3Integration
from common.destinations.excel_destination import ExcelIntegration
from common.destinations.postgres_destination import PostgresIntegration
from fastapi import APIRouter
from application.integrations.destinations.request.request_register_destination import (
    RegisterS3IntegrationRequest,
    RegisterExcelIntegrationRequest,
    RegisterPostgresIntegrationRequest,
)
from application.integrations.destinations.response.response_register_destination import RegisterIntegrationResponse

router = APIRouter()

@router.post("/register_s3", response_model=RegisterIntegrationResponse)
def register_s3_destination(request: RegisterS3IntegrationRequest):
    config = {
        'aws_access_key': request.aws_access_key,
        'aws_secret_key': request.aws_secret_key,
        'bucket_name': request.bucket_name,
        'region_name': request.region_name
    }
    DestinationRepository.register_integration('s3_destination', config, S3Integration)
    return RegisterIntegrationResponse(success=True, message="S3 Destination registered successfully.")

@router.post("/register_excel", response_model=RegisterIntegrationResponse)
def register_excel_destination(request: RegisterExcelIntegrationRequest):
    config = {
        'data_source': request.data_source,
        'output_filename': request.output_filename,
    }
    DestinationRepository.register_integration('excel_destination', config, ExcelIntegration)
    return RegisterIntegrationResponse(success=True, message="Excel Destination registered successfully.")

@router.post("/register_postgres", response_model=RegisterIntegrationResponse)
def register_postgres_destination(request: RegisterPostgresIntegrationRequest):
    config = {
        'host': request.host,
        'port': request.port,
        'database': request.database,
        'user': request.user,
        'password': request.password,
        'schema': request.schema
    }
    DestinationRepository.register_integration('postgres_destination', config, PostgresIntegration)
    return RegisterIntegrationResponse(success=True, message="PostgreSQL Destination registered successfully.")