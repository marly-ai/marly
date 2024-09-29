from typing import Dict, Any
from common.sources.enums.source_enums import SourceType
from common.sources.local_fs_source import LocalFSIntegration
from common.sources.s3_source import S3Integration

class SourceFactory:
    @staticmethod
    def create_source(source_type: str, documents_location: str = None, additional_params: Dict[str, Any] = None):
        if not source_type:
            raise ValueError("source_type must be provided.")

        try:
            source_type_enum = SourceType(source_type.lower())
        except ValueError:
            raise ValueError(f"Invalid source type. Allowed values are: {', '.join([s.value for s in SourceType])}")

        if source_type_enum == SourceType.LOCAL_FS:
            return LocalFSIntegration(
                base_path=documents_location,
                additional_params=additional_params
            )
        elif source_type_enum == SourceType.S3:
            return S3Integration(
                bucket_name=documents_location,
                additional_params=additional_params
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type_enum}")