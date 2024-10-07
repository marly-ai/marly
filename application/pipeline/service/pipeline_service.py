import json
import uuid
import base64
import zlib
import hashlib
from io import BytesIO
from typing import List, Dict, Optional
import redis.asyncio as redis
import logging
import time
import asyncio
from urllib.parse import urlparse
import aiohttp

from application.pipeline.models.models import (
    PipelineRequestModel,
    PipelineResponseModel,
    JobStatus,
    PipelineResult,
    ExtractionRequestModel,
    WorkloadItem
)
from common.text_extraction.text_extractor import get_pdf_page_count
from common.models.model_factory import ModelFactory
from common.sources.source_factory import SourceFactory
from langsmith import Client as LangSmithClient
from langchain.schema import SystemMessage, HumanMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

langsmith_client = LangSmithClient()

async def run_pipeline(customer_input: PipelineRequestModel):
    logger.info("Starting Pipeline Run...")

    try:
        con = await redis.from_url("redis://redis:6379/0", encoding="utf-8", decode_responses=True)
    except Exception as e:
        logger.error(f"Redis connection error: {e}")
        return {
            "error": "Service temporarily unavailable",
            "details": "Unable to connect to the database. Please try again later."
        }

    task_id = str(uuid.uuid4())
    start_time = int(time.time())
    await con.set(f"job-start-time:{task_id}", start_time)
    await con.xadd(
        f"job-status:{task_id}",
        {"status": json.dumps(JobStatus.PENDING.value), "start_time": str(start_time)}
    )
    try:
        ModelFactory.create_model(
            model_type=customer_input.provider_type,
            model_name=customer_input.provider_model_name,
            api_key=customer_input.api_key,
            additional_params=customer_input.additional_params
        )
    except ValueError as e:
        logger.error(f"Model creation error: {e}")
        return {
            "error": "Invalid model configuration",
            "details": str(e)
        }
    
    # Publish details to be used by other workers
    model_details_json = json.dumps({
        "provider_type": customer_input.provider_type,
        "provider_model_name": customer_input.provider_model_name,
        "api_key": customer_input.api_key,
        "markdown_mode": customer_input.markdown_mode,
        "additional_params": customer_input.additional_params
    })
    await con.set("model-details", model_details_json)

    pdf_hash = hashlib.sha256(json.dumps([w.dict() for w in customer_input.workloads]).encode()).hexdigest()
    cache_key = f"cache:{pdf_hash}"
    cached_hash = await con.get(cache_key)
    if cached_hash:
        logger.info(f"Cache hit for key: {cache_key}")
        cached_response = await con.get(cached_hash)
        if cached_response:
            return json.loads(cached_response)

    async def process_workload(index: int, workload_combo: WorkloadItem) -> int:
        try:
            if workload_combo.pdf_stream and workload_combo.data_source:
                logger.error(f"Workload {index} cannot have both pdf_stream and data_source.")
                raise ValueError("Workload cannot have both pdf_stream and data_source.")
            if workload_combo.pdf_stream:
                return await handle_pdf_stream(index, workload_combo, con, task_id)
            elif workload_combo.data_source == "web":
                return await handle_web_source(index, workload_combo, con, task_id)
            elif workload_combo.data_source:
                return await handle_data_source(index, workload_combo, con, task_id)
            else:
                logger.error(f"Workload {index} must have either pdf_stream or data_source.")
                return 0

        except Exception as e:
            logger.error(f"Error processing workload {index}: {e}")
            return 0

    # Process all workloads concurrently
    workload_results = await asyncio.gather(
        *[process_workload(index, workload_combo) for index, workload_combo in enumerate(customer_input.workloads)],
        return_exceptions=False
    )

    total_pages = sum(workload_results)
    logger.info(f"Total pages processed: {total_pages}")

    # Update job status to IN_PROGRESS
    await con.xadd(
        f"job-status:{task_id}",
        {"status": json.dumps(JobStatus.IN_PROGRESS.value)}
    )

    response = PipelineResponseModel(
        message="Tasks submitted successfully",
        task_id=task_id
    )

    response_hash = hashlib.sha256(json.dumps(response.dict()).encode()).hexdigest()
    await con.setex(cache_key, 3600, response_hash)
    await con.setex(response_hash, 3600, json.dumps(response.dict()))

    return {"task_id": response.task_id, "message": response.message}

async def handle_pdf_stream(index: int, workload_combo: WorkloadItem, con: redis.Redis, task_id: str) -> int:
    logger.info(f"Processing workload {index} with pdf_stream.")
    # Decode the base64 encoded PDF stream
    try:
        decompressed_pdf = zlib.decompress(base64.b64decode(workload_combo.pdf_stream))
    except zlib.error as e:
        logger.error(f"Decompression failed for workload {index}: {e}")
        return 0

    pdf_cursor = BytesIO(decompressed_pdf)
    logger.info(f"Decompressed PDF for workload {index}")

    try:
        page_count = get_pdf_page_count(pdf_cursor)
        logger.debug(f"Page count for workload {index}: {page_count}")
    except Exception as e:
        logger.error(f"Error getting page count for workload {index}: {e}")
        return 0

    pdf_key = f"pdf:{task_id}:{index}"
    await con.set(pdf_key, base64.b64encode(decompressed_pdf).decode())
    logger.info(f"PDF stored in Redis with key: {pdf_key} as base64 string")

    schemas = [json.loads(schema) for schema in workload_combo.schemas]

    task_payload = ExtractionRequestModel(
        task_id=task_id,
        pdf_key=pdf_key,
        schemas=schemas
    )

    await con.xadd("extraction-stream", {"payload": json.dumps(task_payload.dict())})

    return page_count

async def fetch_document(session, workload_combo):
    try:
        async with session.get(workload_combo.documents_location) as response:
            response.raise_for_status()  # This will raise an HTTPError for bad responses
            content = await response.read()
            return content
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching document: {str(e)}")
        raise


async def handle_web_source(index: int, workload_combo: WorkloadItem, con: redis.Redis, task_id: str) -> int:
    logger.info(f"Processing workload {index} with web source: {workload_combo.documents_location}")
    
    # Fetch the web content
    # You might want to use a library like aiohttp for asynchronous HTTP requests
    # Here I need to make sure that the url is valid and that it returns a 200
    # Validate URL
    try:
        result = urlparse(workload_combo.documents_location)
        if not all([result.scheme, result.netloc]):
            logger.error(f"Invalid URL: {workload_combo.documents_location}")
            return 0
    except ValueError:
        logger.error(f"Invalid URL format: {workload_combo.documents_location}")
        return 0
    logger.info("URL is valid")
    logger.info("Fetching document content")
    async with aiohttp.ClientSession() as session:
        try:
            document_content = await fetch_document(session, workload_combo)
        except aiohttp.ClientError as e:
            logger.error(f"Failed to connect to URL: {str(e)}")
            return 0
    
    # Process the HTML content as needed
    # For example, you might want to extract text or specific elements
    # This is where the preprocessing could come into play, or I can add it 
    # Store the processed content in Redis
    content_key = f"web:{task_id}:{index}"
    await con.set(content_key, document_content)
    
    # Create and add the extraction task
    schemas = [json.loads(schema) for schema in workload_combo.schemas]
    task_payload = ExtractionRequestModel(
        task_id=task_id,
        pdf_key=content_key,
        schemas=schemas,
        source_type="web"
    )
    await con.xadd("extraction-stream", {"payload": json.dumps(task_payload.dict())})

    return 1


async def handle_data_source(index: int, workload_combo: WorkloadItem, con: redis.Redis, task_id: str) -> int:
    logger.info(f"Processing workload {index} with data_source: {workload_combo.data_source}")
    logger.info(f"Documents location: {workload_combo.documents_location}")
    source = SourceFactory.create_source (
        source_type=workload_combo.data_source,
        documents_location=workload_combo.documents_location,
        additional_params=workload_combo.additional_params
    )
    logger.info(f"Created source: {source}")

    all_files = source.read_all()
    logger.info(f"List of all files: {all_files}")
    if not all_files:
        logger.warning(f"No files found in data source: {workload_combo.data_source}")
        return 0

    relevant_file = await get_relevant_file_via_llm(all_files, workload_combo.file_name)
    if not relevant_file:
        logger.warning(f"LLM did not return a valid file for workload {index}")
        return 0

    logger.info(f"Selected relevant file: {relevant_file}")

    file_stream = source.read({"file_key": relevant_file})
    if not file_stream:
        logger.warning(f"Failed to read the selected file: {relevant_file}")
        return 0

    compressed_data = base64.b64encode(zlib.compress(file_stream.getvalue())).decode('utf-8')

    try:
        decompressed_pdf = zlib.decompress(base64.b64decode(compressed_data))
    except zlib.error as e:
        logger.error(f"Decompression failed for workload {index}: {e}")
        return 0

    pdf_cursor = BytesIO(decompressed_pdf)
    logger.info(f"Decompressed PDF for workload {index}")

    try:
        page_count = get_pdf_page_count(pdf_cursor)
        logger.debug(f"Page count for workload {index}: {page_count}")
    except Exception as e:
        logger.error(f"Error getting page count for workload {index}: {e}")
        return 0

    pdf_key = f"pdf:{task_id}:{index}"
    await con.set(pdf_key, base64.b64encode(decompressed_pdf).decode())
    logger.info(f"PDF stored in Redis with key: {pdf_key} as base64 string")

    schemas = [json.loads(schema) for schema in workload_combo.schemas]

    task_payload = ExtractionRequestModel(
        task_id=task_id,
        pdf_key=pdf_key,
        schemas=schemas
    )

    await con.xadd("extraction-stream", {"payload": json.dumps(task_payload.dict())})

    return page_count

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

async def get_relevant_file_via_llm(filenames: List[str], file_name: str) -> Optional[str]:
    if not filenames:
        logger.error("No filenames provided to determine relevance.")
        return None

    try:
        con = await redis.from_url("redis://redis:6379/0", encoding="utf-8", decode_responses=True)
        customer_input_str = await con.get("model-details")
        logger.info(f"Model details JSON: {customer_input_str}")
        if not customer_input_str:
            logger.error("Model details not found in Redis.")
            return None

        customer_input = json.loads(customer_input_str)

        model_instance = ModelFactory.create_model(
            model_type=customer_input["provider_type"],
            model_name=customer_input["provider_model_name"],
            api_key=customer_input["api_key"],
            additional_params=customer_input["additional_params"]
        )

        prompt = langsmith_client.pull_prompt("marly/get-relevant-file")
        
        messages = prompt.invoke({
            "first_value": file_name,
            "second_value": filenames
        })

        processed_messages = preprocess_messages(messages)

        if not processed_messages:
            logger.error("No messages to process for determining relevant file.")
            return None

        relevant_file = model_instance.do_completion(processed_messages)

        if relevant_file in filenames:
            return relevant_file
        else:
            logger.error(f"LLM returned an invalid filename: {relevant_file}")
            return None

    except Exception as e:
        logger.exception("An error occurred while determining the relevant file via LLM.")
        return None

async def get_results_from_stream(con: redis.Redis, task_id: str) -> List[Dict]:
    entries = await con.xrange(f"job-status:{task_id}")
    results = []
    for _, entry in entries:
        if b'result' in entry:
            result = json.loads(entry[b'result'])
            if 'results' in result:
                results.extend(result['results'])
    return results

async def get_pipeline_results(task_id: str):
    try:
        con = await redis.from_url("redis://redis:6379/0", encoding="utf-8", decode_responses=True)
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return {"error": "Failed to connect to Redis"}, 500

    try:
        status_stream = await con.xrange(f"job-status:{task_id}")
    except Exception as e:
        logger.error(f"Failed to fetch job status: {e}")
        return {"error": "Failed to fetch job status"}, 500

    if not status_stream:
        return {"error": "Task not found"}, 404

    latest_status = JobStatus.PENDING
    total_run_time = 'N/A'
    all_results = []

    for _, entry in status_stream:
        try:
            entry_status = JobStatus(json.loads(entry['status']))
            latest_status = entry_status
        except (json.JSONDecodeError, ValueError):
            logger.error(f"Invalid status value: {entry['status']}")

        if 'total_run_time' in entry:
            total_run_time = entry['total_run_time']

        if 'result' in entry:
            try:
                result_data = json.loads(entry['result'])
                all_results.append(result_data)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse result JSON: {entry['result']}")

    response = PipelineResult(
        task_id=task_id,
        status=latest_status,
        results=all_results,
        total_run_time=total_run_time
    )

    return response.dict()