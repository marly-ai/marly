import PyPDF2
from typing import List
from io import BytesIO
import logging
from dotenv import load_dotenv
from langchain.schema import SystemMessage, HumanMessage
from langsmith import Client
import asyncio
import time
import fitz
from concurrent.futures import ThreadPoolExecutor
from common.prompts.prompt_enums import PromptType
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import os
import hashlib
import redis


load_dotenv()

logger = logging.getLogger(__name__)

def get_pdf_page_count(pdf_stream):
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_stream)
        return len(pdf_reader.pages)
    except PyPDF2.errors.PdfReadError as e:
        raise PyPDF2.errors.PdfReadError(f"Error reading PDF file: {str(e)}")
    
def extract_page_as_markdown_for_relevance(pdf_stream: BytesIO, page_number: int) -> str:
    //#TODO This is the extraction for both relevant pages and processing?
    # This is where I need to use convert pdf_to_image
    page_number -= 1
    with fitz.open(stream=pdf_stream, filetype="pdf") as doc:
        page = doc.load_page(page_number)
        markdown_text = page.get_text("markdown")
        if not markdown_text.strip():
            raise ValueError(f"Page {page_number + 1} does not contain any content.")
    return markdown_text


def extract_page_as_markdown(pdf_stream: BytesIO, page_number: int) -> str:
    # Create a unique identifier for the cache key
    pdf_hash = hashlib.md5(pdf_stream.getvalue()).hexdigest()
    cache_key = f"pdf_content:{pdf_hash}:{page_number}"

    # Initialize Redis client
    redis_client = redis.Redis(host='localhost', port=6379, db=0)  # Adjust connection details as needed

    # Check if content is in cache
    cached_content = redis_client.get(cache_key)
    if cached_content:
        return cached_content.decode('utf-8')
    # Azure Document Intelligence endpoint and key
    endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")

    if not endpoint or not key:
        raise ValueError("Azure Document Intelligence endpoint or key not found in environment variables.")

    # Create the Document Analysis client
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

    # Analyze the document
    pdf_stream.seek(0)
    poller = document_analysis_client.begin_analyze_document("prebuilt-read", pdf_stream)
    result = poller.result()

    # Extract text from the specified page
    # Extract content from the entire document
    document_content = result.content

    # Clean and format the content
    cleaned_content = document_content.strip()  # Remove leading/trailing whitespace
    redis_client.set(cache_key, cleaned_content)

    return cleaned_content
    

def process_page(client, prompt, page_number: int, page_text: str, formatted_keywords: str) -> int:
    try:
        raw_payload = prompt.invoke({"first_value": page_text, "second_value": formatted_keywords})
        
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
            logger.warning(f"Unexpected raw_payload format for page {page_number}: {type(raw_payload)}")
        
        if messages:
            response = client.do_completion(messages)
            logger.info(f"Response for page {page_number}: {response}")
            if isinstance(response, str) and "yes" in response.lower():
                logger.info(f"Page {page_number} is relevant according to the model")
                return page_number
            else:
                logger.info(f"Page {page_number} is not relevant according to the model")
    except Exception as e:
        logger.error(f"Error processing page {page_number}: {e}")
    
    return -1

async def find_common_pages(client, file_stream: BytesIO, formatted_keywords: str) -> List[int]:
    try:
        start_time = time.time()
        pdf_reader = PyPDF2.PdfReader(file_stream)
        langsmith_client = Client()
        prompt = langsmith_client.pull_prompt(PromptType.RELEVANT_PAGE_FINDER.value)
        logger.info(f"MODEL TYPE: {type(client)}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=10) as executor:
            tasks = []
            for page_number in range(len(pdf_reader.pages)):
                page_text = extract_page_as_markdown_for_relevance(file_stream, page_number)
                task = loop.run_in_executor(executor, process_page, client, prompt, page_number, page_text, formatted_keywords)
                tasks.append(task)

            results = await asyncio.gather(*tasks)

        relevant_pages = [page for page in results if page != -1]

        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Processed {len(pdf_reader.pages)} pages in {total_time:.2f} seconds")
        logger.info(f"Relevant Pages: {relevant_pages}")
        
        return relevant_pages
        
    except Exception as e:
        logger.error(f"Error finding common pages: {e}")
        return []