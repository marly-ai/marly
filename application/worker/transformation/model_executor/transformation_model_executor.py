from application.configuration.repositories.model_repository import ModelRepository
from application.configuration.configs.model_config import ModelConfig
from common.observability.observability_decorator import observability
import logging
import ast
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
OBSERVABILITY_PROVIDER = os.getenv('OBSERVABILITY_PROVIDER', '')

def run_transformation(metrics, prompts, model_id, column_locations):
    logger.info(f"Starting transformation for model_id: {model_id}")
    
    model_config = ModelRepository.get(model_id)
    
    if not model_config:
        logger.error(f"Model with ID {model_id} not found")
        raise ValueError(f"Model with ID {model_id} not found")
    
    logger.info(f"Model configuration retrieved: {model_config}")
    
    model_instance = ModelConfig.get_model_instance(model_config)
    logger.info(f"Model instance created: {model_instance}")
    
    clean_column_info = clean_columns(column_locations)
    logger.info(f"Clean column info: {clean_column_info}")
    
    transformed_data = transform_to_output_schema(
        prompts["TRANSFORMATION"],
        metrics,
        clean_column_info,
        model_instance,
        model_config.model_name
    )
    logger.info(f"Data transformation completed: {transformed_data}")
    
    return transformed_data

def clean_columns(column_locations):
    if isinstance(column_locations, str):
        column_locations = ast.literal_eval(column_locations)
    
    columns = column_locations.keys()
    formatted_info = ", ".join(columns)
    return f"[{formatted_info}]"

@observability(OBSERVABILITY_PROVIDER)
def transform_to_output_schema(prompt_config, metrics, clean_column_info, model_instance, model_name):
    return model_instance.do_completion(
        model_name=model_name,
        prompt_config=prompt_config,
        first_value=metrics,
        second_value=clean_column_info
    )
