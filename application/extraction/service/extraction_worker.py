import os
import asyncio
import json
import logging
from typing import Optional, Dict, Any
from redis.asyncio import Redis
from redis.exceptions import RedisError
from datetime import datetime
from application.extraction.models.models import ( ExtractionRequestModel,
    ExtractionResponseModel,
    SchemaResult,
    JobStatus
)
from application.extraction.service.extraction_handler import run_extraction
from common.redis.redis_config import get_redis_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_extractions() -> None:
    logger.info("Starting extraction worker")
    redis = await get_redis_connection()
    last_id = "0-0"

    while True:
        try:
            result = await redis.xread(
                streams={"extraction-stream": last_id},
                count=1,
                block=0
            )
            logger.info(f"Result: {result}")

            for stream_name, messages in result:
                for message_id, message in messages:
                    logger.info(f"Received message from stream {stream_name}: ID {message_id}")
                    payload = message.get(b"payload")
                    if payload:
                        try:
                            logger.info(f"Payload value: {payload}")
                            extraction_request = ExtractionRequestModel(**json.loads(payload.decode('utf-8')))
                            extraction_result = await process_extraction(extraction_request)
                            serialized_result = json.dumps(extraction_result.dict())
                            logger.info(f"Pushing result to transformation-stream: {serialized_result}")
                            await redis.xadd("transformation-stream", {"payload": serialized_result})
                            logger.info("Successfully pushed result to transformation-stream")
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse payload JSON: {e}")
                        except Exception as e:
                            logger.error(f"Error processing extraction task: {e}")
                    else:
                        logger.error("Message does not contain 'payload' field")
                    last_id = message_id

        except RedisError as e:
            logger.error(f"Error reading from Redis stream: {e}")
            await asyncio.sleep(1)

async def process_extraction(extraction_request: ExtractionRequestModel) -> ExtractionResponseModel:
    try:
        results = await run_extraction(extraction_request.pdf_key, extraction_request.schemas)
        schema_results = [
            SchemaResult(
                schema_id=f"schema_{index}",
                metrics={f"schema_{index}": result},
                schema_data=schema
            )
            for index, (schema, result) in enumerate(zip(extraction_request.schemas, results))
        ]

        response = ExtractionResponseModel(
            task_id=extraction_request.task_id,
            pdf_key=extraction_request.pdf_key,
            results=schema_results
        )

        redis = await get_redis_connection()
        await update_job_status(redis, extraction_request.task_id, JobStatus.PENDING, None)

        return response

    except Exception as e:
        logger.error(f"Error processing extraction task: {e}")
        redis = await get_redis_connection()
        # Log the type of the key causing the error
        key_type = await redis.type(extraction_request.pdf_key)
        logger.error(f"Key type for {extraction_request.pdf_key}: {key_type}")

        # Handle unexpected key types
        if key_type != b'string':
            logger.error(f"Unexpected key type for {extraction_request.pdf_key}. Deleting the key.")
            await redis.delete(extraction_request.pdf_key)

        await update_job_status(redis, extraction_request.task_id, JobStatus.FAILED, str(e))
        raise e

async def update_job_status(
    redis: Redis,
    task_id: str,
    status: JobStatus,
    error_message: Optional[str]
) -> None:
    fields: Dict[str, Any] = {"status": status.value}
    if error_message:
        fields["error_message"] = error_message

    start_time_str = await redis.get(f"job-start-time:{task_id}")
    start_time = int(start_time_str) if start_time_str else 0
    current_time = int(datetime.utcnow().timestamp())
    total_run_time = current_time - start_time
    run_time_str = f"{total_run_time // 60} minutes" if total_run_time >= 60 else f"{total_run_time} seconds"
    fields["total_run_time"] = run_time_str

    await redis.xadd(f"job-status:{task_id}", fields)

async def clear_extraction_stream() -> None:
    retries = 0
    max_retries = 5
    delay = 1

    while retries < max_retries:
        try:
            redis = await get_redis_connection()
            await redis.xtrim("extraction-stream", approximate=True, maxlen=0)
            logger.info("Cleared extraction-stream")
            return
        except RedisError as e:
            if "LOADING" in str(e):
                logger.info(f"Redis is still loading. Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                retries += 1
                delay *= 2
            else:
                raise e

    raise Exception("Failed to clear extraction-stream after maximum retries")