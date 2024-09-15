import os
import logging
from dotenv import load_dotenv
import asyncio
from application.transformation.service.transformation_worker import clear_transformation_stream, run_transformations

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    try:
        await clear_transformation_stream()
    except Exception as e:
        logging.error("Failed to clear transformation-stream: %s", e)

    try:
        logging.info("Started transformation service...")
        await run_transformations()
    except Exception as e:
        logging.error("Application error: %s", e)
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
