import asyncio
import logging
from typing import Dict, List
from redis.asyncio import Redis
from common.redis.redis_config import get_redis_connection
from common.models.model_factory import ModelFactory
from application.transformation.models.models import ModelDetails
from langsmith import Client as LangSmithClient
from common.prompts.prompt_enums import PromptType
from langchain.schema import SystemMessage, HumanMessage
from common.destinations.destination_factory import DestinationFactory
from common.destinations.enums.destination_enums import DestinationType
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

langsmith_client = LangSmithClient()

def preprocess_messages(raw_payload):
    messages = []
    if hasattr(raw_payload, 'to_messages'):
        for message in raw_payload.to_messages():
            if isinstance(message, SystemMessage):
                messages.append({"role": "system", "content": message.content})
            elif isinstance(message, HumanMessage):
                messages.append({"role": "user", "content": message.content})
            else:
                logger.warning(f"Unexpected message type: {type(message)}")
    else:
        logger.warning(f"Unexpected raw_payload format: {type(raw_payload)}")
    return messages

async def get_latest_model_details(redis: Redis) -> ModelDetails:
    try:
        model_details_json = await redis.get("model-details")
        if not model_details_json:
            logger.error("No data found for model-details")
            return None

        model_details = ModelDetails(**json.loads(model_details_json))
        return model_details

    except Exception as e:
        logger.error(f"Failed to get or parse model details: {e}")
        return None

async def run_transformation(metrics: Dict[str, str], schema: Dict[str, str], source_type: str) -> Dict[str, str]:
    logger.info("Starting transformation process")

    redis: Redis = await get_redis_connection()
    model_details = await get_latest_model_details(redis)
    if not model_details:
        return {}

    try:
        model_instance = ModelFactory.create_model(
            model_type=model_details.provider_type,
            model_name=model_details.provider_model_name,
            api_key=model_details.api_key,
            additional_params=model_details.additional_params
        )
        markdown_mode = model_details.markdown_mode
        logger.info(f"Model instance created with type: {model_details.provider_type}, name: {model_details.provider_model_name}")
    except ValueError as e:
        logger.error(f"Model creation error: {e}")
        return {}

    schema_keys = ",".join(schema.keys())
    
    tasks = [
        asyncio.create_task(process_schema(model_instance, schema_id, metric_value, schema_keys, markdown_mode, source_type))
        for schema_id, metric_value in metrics.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    transformed_metrics = {schema_id: result for schema_id, result in zip(metrics.keys(), results) if isinstance(result, str)}
    logger.info(f"Transformed metrics: {transformed_metrics}")

    return transformed_metrics

async def process_schema(client, schema_id: str, metric_value: str, schema_keys: str, markdown_mode: bool, source_type: str) -> str:
    try:
        if source_type == 'web':
            prompt = langsmith_client.pull_prompt(PromptType.TRANSFORMATION_WEB.value)
        elif markdown_mode:
            prompt = langsmith_client.pull_prompt(PromptType.TRANSFORMATION_MARKDOWN.value)
        else:
            prompt = langsmith_client.pull_prompt(PromptType.TRANSFORMATION.value)
        
        messages = prompt.invoke({
            "first_value": metric_value,
            "second_value": schema_keys
        })
        processed_messages = preprocess_messages(messages)
        if not processed_messages:
            logger.error("No messages to process for transformation")
            return ""
        if markdown_mode:
            transformed_metric = client.do_completion(processed_messages)
        else:
            transformed_metric = client.do_completion(processed_messages, response_format={"type": "json_object"})
        return transformed_metric
    except Exception as e:
        logger.error(f"Error transforming metric for schema {schema_id}: {e}")
        return ""

async def run_transformation_only(task_id: str, data_location_key: str, schemas: List[str], destination: str, raw_data: str) -> Dict[str, str]:
    # eventually we will want to do destination writes here
    logger.info(f"Starting transformation-only process for task_id: {task_id}")

    redis: Redis = await get_redis_connection()
    model_details = await get_latest_model_details(redis)
    if not model_details:
        return {}

    try:
        model_instance = ModelFactory.create_model(
            model_type=model_details.provider_type,
            model_name=model_details.provider_model_name,
            api_key=model_details.api_key,
            additional_params=model_details.additional_params
        )
        markdown_mode = model_details.markdown_mode
        logger.info(f"Model instance created with type: {model_details.provider_type}, name: {model_details.provider_model_name}")
    except ValueError as e:
        logger.error(f"Model creation error: {e}")
        return {}

    destination_config = {
        "db_path": destination,  
        "additional_params": {} 
    }
    destination_instance = DestinationFactory.create_destination(DestinationType.SQLITE.value, destination_config)
    
    try:
        database_schema = destination_instance.get_table_structure(data_location_key)
    except AttributeError:
        logger.error("The destination does not support get_table_structure method")
        database_schema = {}

    transformed_metrics = {}
    for schema in schemas:
        try:
            logger.info(f"schema: {schema}, raw_data: {raw_data}, database_schema: {database_schema}")
            prompt = langsmith_client.pull_prompt(PromptType.TRANSFORMATION_ONLY.value)
            messages = prompt.invoke({
                "first_value": schema,
                "second_value": raw_data,
                "third_value": database_schema
            })
            processed_messages = preprocess_messages(messages)
            if not processed_messages:
                logger.error(f"No messages to process for transformation of schema: {schema}")
                continue
            if markdown_mode:
                result = model_instance.do_completion(processed_messages)
            else:
                logger.info(f"Transforming schema: {schema} with JSON response format")
                result = model_instance.do_completion(processed_messages, response_format={"type": "json_object"})
            transformed_metrics[schema] = result
        except Exception as e:
            logger.error(f"Error transforming metric for schema {schema}: {e}")

    logger.info(f"Transformed metrics for task_id {task_id}: {transformed_metrics}")
    return transformed_metrics
