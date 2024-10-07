import asyncio
import logging
from typing import List, Dict
from io import BytesIO
from redis.asyncio import Redis
from common.redis.redis_config import get_redis_connection
from common.text_extraction.text_extractor import extract_page_as_markdown, find_common_pages
import base64
from dotenv import load_dotenv
import json
from common.models.model_factory import ModelFactory
from application.extraction.models.models import ModelDetails
from langsmith import Client as LangSmithClient
from common.prompts.prompt_enums import PromptType
from langchain.schema import SystemMessage, HumanMessage
from bs4 import BeautifulSoup
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

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
    
def web_preprocessing(html_content: str) -> str:
    # Parse the HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for script_or_style in soup(["script", "style", "meta", "link"]):
        script_or_style.decompose()

    # Remove navigation, footer, ads, and sidebars
    for element in soup(["nav", "footer", "aside"]):
        element.decompose()

    # Remove elements with common ad-related class names
    ad_classes = ["ad", "advertisement", "banner", "sidebar"]
    for element in soup.find_all(class_=lambda x: x and any(cls in x for cls in ad_classes)):
        element.decompose()

    # Try to find the main content
    main_content = soup.find("main") or soup.find("article")
    if not main_content:
        # If no main or article tag, look for the largest text-containing div
        main_content = max(
            soup.find_all("div", text=True),
            key=lambda div: len(div.get_text()),
            default=soup
        )

    # Process links in the main content
    for a in main_content.find_all('a', href=True):
        href = a['href']
        if not a.string:
            a.string = href
        else:
            a.string = f"{a.string} ({href})"

    # Extract text from the main content
    text = main_content.get_text(separator=' ', strip=True)

    # Clean the text
    text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
    text = re.sub(r'&[a-zA-Z]+;', '', text)  # Remove HTML entities
    logger.info(f"Length of text: {len(text)}")
    return text

async def run_web_extraction(pdf_key: str, schemas: List[Dict[str, str]]) -> List[str]:
    logger.info(f"Starting web extraction process for pdf_key: {pdf_key}")
    
    # Retrieve the HTML content from Redis
    redis: Redis = await get_redis_connection()
    html_content = await redis.get(pdf_key)
    
    if not html_content:
        logger.error(f"No HTML content found for key: {pdf_key}")
        return []

    # Preprocess the HTML content
    preprocessed_text = web_preprocessing(html_content.decode('utf-8'))
    
    # Get the latest model details
    model_details = await get_latest_model_details(redis)
    if not model_details:
        return []

    # Create model instance
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

    # Need to change this
    # Process each schema
    results = []
    for schema in schemas:
        try:
            formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schema.items()])
            prompt = langsmith_client.pull_prompt(PromptType.EXTRACTION.value)
            messages = prompt.invoke({
                "first_value": preprocessed_text,
                "second_value": formatted_keywords,
                "third_value": ""  # No examples for web extraction
            })
            processed_messages = preprocess_messages(messages)
            if not processed_messages:
                logger.error("No messages to process for LLM call")
                continue
            response = model_instance.do_completion(processed_messages)
            results.append(response)
        except Exception as e:
            logger.error(f"Error processing schema: {e}")

    return results

async def run_extraction(pdf_key: str, schemas: List[Dict[str, str]]) -> List[str]:
    logger.info(f"Starting extraction process for pdf_key: {pdf_key}")

    file_stream = await get_file_stream(pdf_key)
    logger.info(f"File stream retrieved. Size: {file_stream.getbuffer().nbytes}")

    redis: Redis = await get_redis_connection()
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
        logger.info(f"Model instance created with type: {model_details.provider_type}, name: {model_details.provider_model_name}, API key: {model_details.api_key}, additional_params: {model_details.additional_params}")
    except ValueError as e:
        logger.error(f"Model creation error: {e}")
        return []

    tasks = [
        asyncio.create_task(process_schema(model_instance, file_stream, schema))
        for schema in schemas
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_metrics = [result for result in results if isinstance(result, str)]

    return all_metrics

async def get_file_stream(pdf_key: str) -> BytesIO:
    logger.info("Entered the file stream function")

    redis: Redis = await get_redis_connection()
    try:
        base64_string: str = await redis.get(pdf_key)
        logger.info(f"Retrieved base64 string from Redis. Length: {len(base64_string)}")
    except Exception as e:
        logger.error(f"Failed to retrieve base64 string from Redis: {e}")
        raise e

    try:
        decoded_bytes = base64.b64decode(base64_string)
        logger.info(f"Decoded base64 string. Byte length: {len(decoded_bytes)}")
    except Exception as e:
        logger.error(f"Failed to decode base64 string: {e}. Base64 string: {base64_string}")
        raise e

    return BytesIO(decoded_bytes)

async def process_schema(client, file_stream: BytesIO, schema: Dict[str, str]) -> str:
    formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schema.items()])
    logger.info(f"Formatted keywords: {formatted_keywords}")

    try:
        page_numbers = await get_relevant_page_numbers(client,file_stream, formatted_keywords)
        logger.info(f"Page numbers: {page_numbers.pages}")
    except Exception as e:
        logger.error(f"Error getting relevant page numbers: {e}")
        return ""

    try:
        examples = await get_examples(client, formatted_keywords)
        examples = examples if examples else ""
        logger.info(f"Examples: {examples}")
    except Exception as e:
        logger.error(f"Error getting examples: {e}")
        examples = ""

    try:
        metrics = await retrieve_multi_page_metrics(page_numbers.pages, formatted_keywords, file_stream, examples, client)
        metrics = metrics if metrics else ""
        logger.info(f"Metrics: {metrics}")
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        metrics = ""

    return metrics

async def get_examples(client, formatted_keywords: str) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.EXAMPLE_GENERATION.value)
        messages = prompt.invoke({"first_value": formatted_keywords})
        processed_messages = preprocess_messages(messages)
        if not processed_messages:
            logger.error("No messages to process for example generation")
            return ""
        examples = client.do_completion(processed_messages)
        return examples
    except Exception as e:
        logger.error(f"Error generating examples: {e}")
        return ""

async def get_relevant_page_numbers(client, file_stream: BytesIO, formatted_keywords: str) -> 'PageNumbers':
    try:
        common_pages = await find_common_pages(client, file_stream, formatted_keywords)
        return PageNumbers(pages=common_pages)
    except Exception as e:
        logger.error(f"Error with find_common_pages: {e}")
        raise Exception("Cannot process: No common pages found or unreadable file.")

async def retrieve_multi_page_metrics(
    pages: List[int],
    keywords: str,
    file_stream: BytesIO,
    examples: str,
    client
) -> str:
    extracted_contents = []

    for page in pages:
        try:
            content = await process_page(file_stream, page)
            if content:
                extracted_contents.append(content)
        except Exception as e:
            logger.error(f"Error processing page {page}: {e}")

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
    return await validate_metrics(combined_results, examples, client)

async def process_page(file_stream: BytesIO, page: int) -> str:
    try:
        content = extract_page_as_markdown(file_stream, page)
        if not content:
            content = extract_page_as_markdown(file_stream, page)
        return content.decode('utf-8') if isinstance(content, bytes) else content
    except Exception as e:
        logger.error(f"Error extracting page {page} as markdown: {e}")
        return ""

async def call_llm_with_file_content(
    formatted_content: str,
    keywords: str,
    examples: str,
    client
) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.EXTRACTION.value)
        messages = prompt.invoke({
            "first_value": formatted_content,
            "second_value": keywords,
            "third_value": examples
        })
        processed_messages = preprocess_messages(messages)
        if not processed_messages:
            logger.error("No messages to process for LLM call")
            return ""
        response = client.do_completion(processed_messages)
        return response
    except Exception as e:
        logger.error(f"Error calling LLM with file content: {e}")
        return ""

async def validate_metrics(
    llm_results: str,
    examples: str,
    client
) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.VALIDATION.value)
        messages = prompt.invoke({
            "first_value": llm_results,
            "second_value": examples
        })
        processed_messages = preprocess_messages(messages)
        if not processed_messages:
            logger.error("No messages to process for metric validation")
            return ""
        validation = client.do_completion(processed_messages)
        return validation
    except Exception as e:
        logger.error(f"Error validating metrics: {e}")
        return ""

class PageNumbers:
    def __init__(self, pages: List[int]):
        self.pages = pages