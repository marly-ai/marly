import PyPDF2
from typing import List
from io import BytesIO
import logging
from dotenv import load_dotenv
from langchain.schema import SystemMessage, HumanMessage
from langsmith import Client
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from common.prompts.prompt_enums import PromptType
import json
from datetime import datetime
from common.redis.redis_config import get_redis_connection

load_dotenv()

logger = logging.getLogger(__name__)

def get_pdf_page_count(pdf_stream):
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_stream)
        return len(pdf_reader.pages)
    except PyPDF2.errors.PdfReadError as e:
        raise PyPDF2.errors.PdfReadError(f"Error reading PDF file: {str(e)}")
    
def extract_page_as_markdown(file_stream: BytesIO, page_number: int) -> str:
    import os
    from PyPDF2 import PdfReader, PdfWriter
    from markitdown import MarkItDown
    
    temp_file = "temp.pdf"
    
    try:
        if not isinstance(page_number, int) or page_number < 0:
            raise ValueError(f"Invalid page number: {page_number}")

        reader = PdfReader(file_stream)
        
        if page_number >= len(reader.pages):
            raise ValueError(f"Page number {page_number} exceeds document length of {len(reader.pages)} pages")

        writer = PdfWriter()
        writer.add_page(reader.pages[page_number])
        
        try:
            with open(temp_file, 'wb') as output_file:
                writer.write(output_file)
        except IOError as e:
            raise IOError(f"Failed to write temporary PDF file: {str(e)}")

        try:
            markitdown = MarkItDown()
            result = markitdown.convert(temp_file)
            
            if not result or not result.text_content:
                logger.warning(f"No text content extracted from page {page_number}")
                return ""
                
            return result.text_content
            
        except Exception as e:
            raise Exception(f"Error converting PDF to markdown: {str(e)}")

    except Exception as e:
        logger.error(f"Error in extract_page_markdown: {str(e)}")
        raise

    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception as e:
            logger.error(f"Failed to remove temporary file {temp_file}: {str(e)}")

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

def process_page(client, prompt, page_number: int, page_text: str, formatted_keywords: str, plan: str) -> tuple[int, dict]:
    try:
        raw_payload = prompt.invoke({
            "first_value": page_text, 
            "second_value": formatted_keywords,
            "third_value": plan
        })
        
        processed_messages = preprocess_messages(raw_payload)
        if processed_messages:
            response = client.do_completion(processed_messages)
            logger.info(f"Response for page {page_number}: {response}")
            
            response_data = {
                'response': response,
                'page_text': page_text,
                'keywords': formatted_keywords,
                'timestamp': datetime.now().isoformat(),
                'is_relevant': "yes" in response[:100].lower()
            }
            
            # Return response_data for all pages, but only mark relevant pages with their page number
            return (page_number if "yes" in response[:100].lower() else -1), response_data
            
    except Exception as e:
        logger.error(f"Error processing page {page_number}: {e}")
        return -1, {
            'response': f"Error: {str(e)}",
            'page_text': page_text,
            'keywords': formatted_keywords,
            'timestamp': datetime.now().isoformat(),
            'is_relevant': False,
            'error': True
        }

async def get_plan(langsmith_client, client, formatted_keywords: str) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.PLAN.value)
        messages = prompt.invoke({"first_value": formatted_keywords})
        processed_messages = preprocess_messages(messages)
        if processed_messages:
            return client.do_completion(processed_messages)
    except Exception as e:
        logger.error(f"Error getting plan: {e}")
        return ""

async def find_common_pages(client, file_stream: BytesIO, formatted_keywords: str) -> List[int]:
    try:
        start_time = time.time()
        pdf_reader = PyPDF2.PdfReader(file_stream)
        langsmith_client = Client()
        prompt = langsmith_client.pull_prompt(PromptType.RELEVANT_PAGE_FINDER_WITH_PLAN.value)
        logger.info(f"MODEL TYPE: {type(client)}")

        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        plan = await get_plan(langsmith_client, client, formatted_keywords)
        logger.info(f"PLAN: {plan}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=10) as executor:
            tasks = []
            for page_number in range(len(pdf_reader.pages)):
                page_text = extract_page_as_markdown(file_stream, page_number)
                task = loop.run_in_executor(
                    executor, 
                    process_page, 
                    client, 
                    prompt, 
                    page_number, 
                    page_text, 
                    formatted_keywords, 
                    plan
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks)

        # Process results and store in Redis
        page_responses = {}
        relevant_pages = []
        
        logger.info(f"Processing {len(results)} results")
        
        for page_number, response_data in results:
            # Store response data for EVERY page, using the loop index as page number
            # since response_data will always exist (even for errors)
            page_responses[str(page_number if page_number != -1 else results.index((page_number, response_data)))] = response_data
            
            # Only add to relevant_pages if it was marked as relevant
            if page_number != -1:
                relevant_pages.append(page_number)

        logger.info(f"Collected responses for {len(page_responses)} pages")

        # Store all responses for this run in Redis
        if page_responses:
            try:
                redis_client = await get_redis_connection()
                redis_key = f"page_responses:{run_id}"
                data_to_store = json.dumps(page_responses)
                
                await redis_client.set(redis_key, data_to_store)
                await redis_client.expire(redis_key, 86400)  # 1 day TTL
                
                stored_data = await redis_client.get(redis_key)
                if stored_data:
                    logger.info(f"Successfully stored and verified data in Redis for run_id: {run_id}")
                else:
                    logger.error("Failed to verify data storage in Redis")
                    
            except Exception as e:
                logger.error(f"Redis storage error: {str(e)}")
        else:
            logger.warning("No page responses to store in Redis")

        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Processed {len(pdf_reader.pages)} pages in {total_time:.2f} seconds")
        logger.info(f"Relevant Pages: {relevant_pages}")
        
        return relevant_pages
        
    except Exception as e:
        logger.error(f"Error finding common pages: {e}")
        return []