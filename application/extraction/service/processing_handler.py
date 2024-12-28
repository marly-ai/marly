import logging
from typing import List, Dict, Optional
from redis.asyncio import Redis
from bs4 import BeautifulSoup
import re
from common.models.model_factory import ModelFactory
from application.extraction.models.models import ModelDetails
from langsmith import Client as LangSmithClient
from common.prompts.prompt_enums import PromptType
from langchain_core.messages import SystemMessage, HumanMessage
import json
from common.agents.prs_agent import process_extraction

logger = logging.getLogger(__name__)
langsmith_client = LangSmithClient()

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

async def get_latest_model_details(redis: Redis) -> Optional[ModelDetails]:
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

async def process_web_content(redis: Redis, pdf_key: str, schemas: List[Dict[str, str]]) -> List[str]:
    logger.info(f"Starting web extraction process for pdf_key: {pdf_key}")
    
    # Retrieve the HTML content from Redis
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

    # Process each schema using PRS agent
    results = []
    for schema in schemas:
        try:
            formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schema.items()])
            # Get example format from the schema
            example_format = await get_example_format(model_instance, formatted_keywords)
            # Use PRS agent for extraction
            result = process_extraction(preprocessed_text, formatted_keywords, example_format, model_instance)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing schema: {e}")
            
    return results

async def get_example_format(client, formatted_keywords: str) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.EXAMPLE_GENERATION.value)
        messages = prompt.invoke({"first_value": formatted_keywords})
        processed_messages = preprocess_messages(messages)
        if processed_messages:
            return client.do_completion(processed_messages)
    except Exception as e:
        logger.error(f"Error generating example format: {e}")
    return ""

def preprocess_messages(raw_payload) -> List[Dict[str, str]]:
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