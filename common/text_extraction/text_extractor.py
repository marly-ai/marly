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
import fitz  # PyMuPDF
from PIL import Image
import io
import base64

load_dotenv()

logger = logging.getLogger(__name__)

def get_pdf_page_count(pdf_stream):
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_stream)
        return len(pdf_reader.pages)
    except PyPDF2.errors.PdfReadError as e:
        raise PyPDF2.errors.PdfReadError(f"Error reading PDF file: {str(e)}")
    
def extract_page_as_markdown_for_relevance(pdf_stream: BytesIO, page_number: int) -> str:
    #TODO This is the extraction for both relevant pages and processing?
    # This is where I need to use convert pdf_to_image
    if not pdf_stream or page_number is None:
        return ""

    try:
        # Reset the stream position
        pdf_stream.seek(0)
        pdf_document = fitz.open(stream=pdf_stream, filetype="pdf")

        # Check if the page number is valid
        if page_number < 0 or page_number >= len(pdf_document):
            return "Invalid Page Number"

        # Get the specified page
        page = pdf_document.load_page(page_number)
        pix = page.get_pixmap()

        # Convert to PNG
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')

        # Add the data:image/png;base64, prefix
        img_str_with_prefix = f"data:image/png;base64,{img_str}"

        return img_str_with_prefix

    except Exception as e:
        return "error"




    # page_number -= 1
    # with fitz.open(stream=pdf_stream, filetype="pdf") as doc:
    #     page = doc.load_page(page_number)
    #     markdown_text = page.get_text("markdown")
    #     if not markdown_text.strip():
    #         raise ValueError(f"Page {page_number + 1} does not contain any content.")
    # return markdown_text


# def extract_page_as_markdown(pdf_stream: BytesIO, page_number: int) -> str:
#     # Create a unique identifier for the cache key
#     pdf_hash = hashlib.md5(pdf_stream.getvalue()).hexdigest()
#     cache_key = f"pdf_content:{pdf_hash}:{page_number}"
#     logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
#     # Initialize Redis client
#     redis_client = redis.Redis(host='redis', port=6379, db=0)  # Adjust connection details as needed

#     # Check if content is in cache
#     cached_content = redis_client.get(cache_key)
#     if cached_content:
#         return cached_content.decode('utf-8')
#     # Azure Document Intelligence endpoint and key
#     endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
#     key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")

#     if not endpoint or not key:
#         raise ValueError("Azure Document Intelligence endpoint or key not found in environment variables.")

#     # Create the Document Analysis client
#     document_analysis_client = DocumentAnalysisClient(
#         endpoint=endpoint, credential=AzureKeyCredential(key)
#     )

#     # Analyze the document
#     pdf_stream.seek(0)
#     poller = document_analysis_client.begin_analyze_document("prebuilt-read", pdf_stream)
#     result = poller.result()

#     # Extract text from the specified page
#     # Extract content from the entire document
#     document_content = result.content

#     # Clean and format the content
#     cleaned_content = document_content.strip()  # Remove leading/trailing whitespace
#     redis_client.set(cache_key, cleaned_content)

#     return cleaned_content

def extract_page_as_markdown(pdf_stream: BytesIO, page_number: int) -> str:
    # ... existing code ...
        # Create a unique identifier for the cache key
    pdf_hash = hashlib.md5(pdf_stream.getvalue()).hexdigest()
    cache_key = f"pdf_content:{pdf_hash}:{page_number}"
    logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
    # Initialize Redis client
    redis_client = redis.Redis(host='redis', port=6379, db=0)  # Adjust connection details as needed

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

    # Analyze the document using the Form Recognizer model
    pdf_stream.seek(0)
    poller = document_analysis_client.begin_analyze_document("prebuilt-layout", pdf_stream)
    result = poller.result()

    # Extract content from the entire document
    document_content = ""
    for page in result.pages:
        for line in page.lines:
            document_content += line.content + "\n"

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
            # response = client.do_completion(messages)
            from portkey_ai import Portkey
            client = Portkey(
                api_key=os.getenv("PORTKEY_API_KEY"),
                virtual_key=os.getenv("PORTKEY_VIRTUAL_KEY")
            )
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing images to see if theres a match for a set of search terms and their given descriptions."},
                    {"role": "user", "content": [
                        {"type": "text",
                         "text": f"""Rules:

Search Criteria: Identify both exact matches and partial matches of the search terms. Partial matches should be considered if they closely resemble the search terms, taking into account variations in wording or labeling.

Associated Values: Ensure that each identified match (whether exact or partial) has a corresponding value on the page. A term without an associated value is not considered a valid match.

Full Presence: For a "Yes" response, all search terms must be present with values, either as exact matches or suitable partial matches.

Format: The response must start with a "Yes" or "No" based on your evaluation do not include any "*" in your response

Instructions:

Given a search term and a description of that search term. Example "Home": "The place where a person sleeps"

Search the page for both exact matches and close variations of each search term provided.

Verify that each identified search term, whether an exact or partial match, has a corresponding value associated with it.

Task: Respond with "Yes" only if every search term (or its close variant) is found with an associated value. If any search term is missing or lacks an associated value, respond with "No."

Examples:

Example 1:
Search Terms: Term1, Term2, Term3
Page Content: Term1: Value1, Term2: Value2, Term3: Value3
Response: Yes (All terms have values)

Example 2:
Search Terms: Term1, Term2, Term3
Page Content: Term1: Value1, Term2: Value2
Response: No (Term3 is missing)

Example 3:
Search Terms: Term1, Term2, Term3
Page Content: Term1: Value1, Term2: Term3: Value3
Response: No (Term2 has no value)

Search Terms: {formatted_keywords}

Start the response with an answer then prove why with a list of exact and partial matches."""},
                        {"type": "image_url", "image_url": {
                            "url": f"{page_text}"}
                         }
                    ]}
                ],
                temperature=0.0,
            )
            response = response.choices[0].message.content
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
