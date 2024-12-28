import asyncio
import logging
from typing import List, Dict, NamedTuple
from io import BytesIO
from redis.asyncio import Redis
from common.redis.redis_config import get_redis_connection
from common.text_extraction.text_extractor import extract_page_as_markdown, find_common_pages
import base64
from dotenv import load_dotenv
from common.models.model_factory import ModelFactory
from langsmith import Client as LangSmithClient
from common.prompts.prompt_enums import PromptType
from application.extraction.service.processing_handler import (
    process_web_content,
    get_latest_model_details,
    preprocess_messages
)
from common.agents.prs_agent import process_extraction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

langsmith_client = LangSmithClient()

class PageNumbers(NamedTuple):
    pages: List[int]

async def run_web_extraction(pdf_key: str, schemas: List[Dict[str, str]]) -> List[str]:
    redis = await get_redis_connection()
    return await process_web_content(redis, pdf_key, schemas)

async def run_extraction(pdf_key: str, schemas: List[Dict[str, str]]) -> List[str]:
    logger.info(f"Starting extraction process for pdf_key: {pdf_key}")
    redis = await get_redis_connection()

    file_stream = await get_file_stream(redis, pdf_key)
    logger.info(f"File stream retrieved. Size: {file_stream.getbuffer().nbytes}")

    model_details = await get_latest_model_details(redis)
    if not model_details:
        return []

    try:
        model_instance = ModelFactory.create_model(
            model_type=model_details.provider_type,
            model_name=model_details.provider_model_name,
            api_key=model_details.api_key,
            additional_params=model_details.additional_params
        )
    except ValueError as e:
        logger.error(f"Model creation error: {e}")
        return []

    tasks = [
        asyncio.create_task(process_schema(model_instance, file_stream, schema))
        for schema in schemas
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [result for result in results if isinstance(result, str)]

async def get_file_stream(redis: Redis, pdf_key: str) -> BytesIO:
    try:
        base64_string: str = await redis.get(pdf_key)
        decoded_bytes = base64.b64decode(base64_string)
        return BytesIO(decoded_bytes)
    except Exception as e:
        logger.error(f"Failed to process file stream: {e}")
        raise e

async def process_schema(client, file_stream: BytesIO, schema: Dict[str, str]) -> str:
    formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schema.items()])
    
    try:
        page_numbers = await get_relevant_page_numbers(client, file_stream, formatted_keywords)
        examples = await get_examples(client, formatted_keywords)
        metrics = await retrieve_multi_page_metrics(
            page_numbers.pages, formatted_keywords, file_stream, 
            examples if examples else "", client
        )
        return metrics if metrics else ""
    except Exception as e:
        logger.error(f"Error processing schema: {e}")
        return ""

async def get_examples(client, formatted_keywords: str) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.EXAMPLE_GENERATION.value)
        messages = prompt.invoke({"first_value": formatted_keywords})
        processed_messages = preprocess_messages(messages)
        if processed_messages:
            return client.do_completion(processed_messages)
    except Exception as e:
        logger.error(f"Error generating examples: {e}")
    return ""

async def get_relevant_page_numbers(client, file_stream: BytesIO, formatted_keywords: str) -> PageNumbers:
    try:
        common_pages = await find_common_pages(client, file_stream, formatted_keywords)
        return PageNumbers(pages=common_pages)
    except Exception as e:
        logger.error(f"Error with find_common_pages: {e}")
        raise Exception("Cannot process: No common pages found or unreadable file.")

async def retrieve_multi_page_metrics(
    pages: List[int], keywords: str, file_stream: BytesIO, examples: str, client
) -> str:
    extracted_contents = []
    for page in pages:
        content = await process_page(file_stream, page)
        if content:
            extracted_contents.append(content)

    if not extracted_contents:
        logger.error("No content extracted from pages")
        return ""

    tasks = [
        asyncio.create_task(call_llm_with_file_content(content, keywords, examples, client))
        for content in extracted_contents
    ]

    llm_results = []
    for task in tasks:
        try:
            result = await task
            if result:
                llm_results.append(result)
        except Exception as e:
            logger.error(f"Error in LLM processing: {e}")

    combined_results = "\n".join(llm_results)
    
    return combined_results

async def process_page(file_stream: BytesIO, page: int) -> str:
    try:
        content = extract_page_as_markdown(file_stream, page)
        return content.decode('utf-8') if isinstance(content, bytes) else content
    except Exception as e:
        logger.error(f"Error extracting page {page} as markdown: {e}")
        return ""

async def call_llm_with_file_content(formatted_content: str, keywords: str, examples: str, client) -> str:
    try:
        return process_extraction(formatted_content, keywords, examples, client)
    except Exception as e:
        logger.error(f"Error calling LLM with file content: {e}")
    return ""