import fitz
from concurrent.futures import ThreadPoolExecutor
import re
import tempfile
import base64
import logging
from common.observability.observability_decorator import observability
from dotenv import load_dotenv
import os

load_dotenv()

global OBSERVABILITY_PROVIDER
OBSERVABILITY_PROVIDER = os.getenv('OBSERVABILITY_PROVIDER', '')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_text_pymupdf(pdf_content, page_number):
    page_number -= 1
    with fitz.open(stream=pdf_content, filetype="pdf") as doc:
        page = doc.load_page(page_number)
        text = page.get_text()
        if not text.strip():
            raise ValueError(f"Page {page_number + 1} does not contain text or is not recognized as a text page.")
    return text

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")

def pdf_page_to_image(pdf_content, page_number):
    with fitz.open(stream=pdf_content, filetype="pdf") as doc:
        page = doc.load_page(page_number - 1)
        pix = page.get_pixmap()
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_image:
            pix.save(temp_image.name)
            base64_image = encode_image(temp_image.name)
    return base64_image

@observability(OBSERVABILITY_PROVIDER)
def check_page_with_llm_image(prompt_config, formatted_search_terms, encoded_image, model_instance, model_name):
    return model_instance.do_completion(
        prompt_config=prompt_config,
        model_name=model_name,
        first_value=formatted_search_terms,
        second_value=encoded_image
    )

def process_page(args):
    prompt_config, pdf_content, formatted_search_terms, i, model_instance, model_name = args
    try:
        encoded_image = pdf_page_to_image(pdf_content, i + 1)
        response = check_page_with_llm_image(prompt_config, formatted_search_terms, encoded_image, model_instance, model_name)
        logger.info(f"Response for page {i + 1}: {response}")
        if "yes" in response.lower():
            return i + 1
    except Exception as e:
        logger.error(f"Error processing page {i + 1}: {e}")
    return None

def find_common_pages(file_blob, formatted_keywords, prompt_config, model_instance, model_name):
    # Convert the stream to bytes
    if hasattr(file_blob, 'read'):
        pdf_content = file_blob.read()
    else:
        pdf_content = file_blob

    with fitz.open(stream=pdf_content, filetype="pdf") as doc:
        num_pages = len(doc)

    common_pages = []
    max_pages_to_process = min(num_pages - 1, 25)
    logger.debug(f"Processing {max_pages_to_process} pages")

    def process_single_page(i):
        return process_page((prompt_config, pdf_content, formatted_keywords, i, model_instance, model_name))

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_single_page, range(1, max_pages_to_process + 1)))

    for result in results:
        if result:
            common_pages.append(result)

    return common_pages if common_pages else None

def extract_page_as_markdown(pdf_content, page_number):
    page_number -= 1
    with fitz.open(stream=pdf_content, filetype="pdf") as doc:
        page = doc.load_page(page_number)
        markdown_text = page.get_text("markdown")
        if not markdown_text.strip():
            raise ValueError(f"Page {page_number + 1} does not contain any content.")
    return markdown_text

def extract_pages_as_markdown(pdf_content, page_numbers):
    def process_page(page_number):
        return extract_page_as_markdown(pdf_content, page_number)

    results = {}
    with ThreadPoolExecutor() as executor:
        future_to_page = {executor.submit(process_page, page_number): page_number for page_number in page_numbers}
        for future in future_to_page:
            page_number = future_to_page[future]
            try:
                results[page_number] = future.result(timeout=5)
            except TimeoutError:
                results[page_number] = "TimeoutError: Processing took too long."
            except Exception as exc:
                results[page_number] = str(exc)

    return results

def metric_in_text(metric, text):
    if " " in metric:
        pattern = ".*".join(re.escape(word) for word in metric.split())
        return re.search(pattern, text, re.DOTALL) is not None
    else:
        return metric in text

def find_common_pages_old(pdf_content, search_terms):
    metrics_pages = {metric: set() for metric in search_terms}

    with fitz.open(stream=pdf_content, filetype="pdf") as doc:
        num_pages = len(doc)

        for i in range(num_pages):
            try:
                text_pymupdf = extract_text_pymupdf(pdf_content, i + 1)
                
                for metric in search_terms:
                    if metric_in_text(metric, text_pymupdf):
                        metrics_pages[metric].add(i + 1)
            except ValueError:
                continue
    
    page_metric_count = {}
    for metric, pages in metrics_pages.items():
        for page in pages:
            if page not in page_metric_count:
                page_metric_count[page] = set()
            page_metric_count[page].add(metric)

    coverage = {page: len(metrics) for page, metrics in page_metric_count.items()}
    sorted_pages = sorted(coverage.items(), key=lambda item: item[1], reverse=True)

    max_coverage = sorted_pages[0][1] if sorted_pages else 0
    ranked_pages = [page for page, cov in sorted_pages if cov == max_coverage]

    return ranked_pages if ranked_pages else []

def extract_markdown(pdf_content, page_number):
    try:
        markdown_text = extract_page_as_markdown(pdf_content, page_number)
        if not markdown_text.strip():
            logger.warning(f"Page {page_number} does not contain any text or is not recognized as a text document.")
            raise ValueError(f"Page {page_number} does not contain any text or is not recognized as a text document.")
        
        logger.debug(f"Successfully extracted markdown from page {page_number}")
        return markdown_text.strip()
    except Exception as e:
        logger.error(f"Error extracting markdown from page {page_number}: {str(e)}")
        raise
