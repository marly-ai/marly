import os
import logging
from dotenv import load_dotenv
import asyncio
from application.extraction.service.extraction_worker import clear_extraction_stream, run_extractions

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    try:
        await clear_extraction_stream()
    except Exception as e:
        logging.error("Failed to clear extraction-stream: %s", e)

    try:
        logging.info("Started extraction service...")
        await run_extractions()
    except Exception as e:
        logging.error("Application error: %s", e)
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
