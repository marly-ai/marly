from langchain_core.tools import tool
from marly import Marly
from dotenv import load_dotenv
import time
import json
import os
import logging
import base64
import zlib
import sqlite3
import pandas as pd

load_dotenv()

BASE_URL = "http://localhost:8100"
PDF_FILE_PATH = "../example_scripts/lacers_reduced.pdf"
CLIENT = Marly(base_url=BASE_URL)

# Define schema for Marly, table column name and a brief description of the column
SCHEMA_1 = {
        "Firm": "The name of the firm",
        "Number of Funds": "The number of funds managed by the firm",
        "Commitment": "The commitment amount in millions of dollars",
        "Percent of Total Comm": "The percentage of total commitment",
        "Exposure (FMV + Unfunded)": "The exposure including fair market value and unfunded commitments in millions of dollars",
        "Percent of Total Exposure": "The percentage of total exposure",
        "TVPI": "Total Value to Paid-In multiple",
        "Net IRR": "Net Internal Rate of Return as a percentage"
    }

# Helper function to read and encode the pdf file
def read_and_encode_pdf(file_path):
    with open(file_path, "rb") as file:
        pdf_content = base64.b64encode(zlib.compress(file.read())).decode('utf-8')
    logging.debug(f"{file_path} read and encoded")
    return pdf_content

def display_table():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('etl_workflow.db')
        
        # Use pandas to read the table into a DataFrame
        df = pd.read_sql_query("SELECT * FROM private_equity_firms", conn)
        
        # Close the connection
        conn.close()
        
        # Print the DataFrame in a pretty format
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error displaying table: {str(e)}")

# Main function to process the pdf file
def process_pdf(file_path):
    """ Processes a PDF file using Marly and returns a JSON string."""
    pdf_content = read_and_encode_pdf(file_path)

    try:
        pipeline_response = CLIENT.pipelines.create(
            api_key=os.getenv("CEREBRAS_API_KEY"),
            provider_model_name="llama3.1-70b",
            provider_type="cerebras",
            workloads=[{"pdf_stream": pdf_content, "schemas": [json.dumps(SCHEMA_1)]}],
        )

        while True:
            results = CLIENT.pipelines.retrieve(pipeline_response.task_id)
            if results.status == 'COMPLETED':
                return [json.loads(results.results[0].metrics[f'schema_{i}']) for i in range(len(results.results[0].metrics))]
            elif results.status == 'FAILED':
                return None
            time.sleep(15)

    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None

def load_data(input_data) -> str:
    try:
        # Debug: Print the input_data type and content
        print(f"Input Data Type: {type(input_data)}")
        print(f"Input Data Content: {input_data}")

        # Ensure input_data is a list
        if not isinstance(input_data, list):
            return "Unsupported input data format: input_data is not a list."

        # Case 1: List contains a single dictionary with lists as values
        if (len(input_data) == 1 and isinstance(input_data[0], dict) and 
            all(isinstance(v, list) for v in input_data[0].values())):
            input_dict = input_data[0]
            keys = input_dict.keys()

            # Check that all lists have the same length
            list_lengths = [len(v) for v in input_dict.values()]
            if len(set(list_lengths)) != 1:
                return "All lists in the input dictionary must have the same length."

            # Transform the input data into a list of dictionaries
            data = [dict(zip(keys, values)) for values in zip(*input_dict.values())]
            print(f"Transformed Data: {data}")

        # Case 2: List contains multiple dictionaries with scalar values
        elif all(isinstance(item, dict) for item in input_data):
            data = input_data
            print(f"Data is a list of dictionaries: {data}")

        else:
            return "Unsupported input data format."

        # Connect to the SQLite database
        conn = sqlite3.connect('etl_workflow.db')
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS private_equity_firms (
                Firm TEXT PRIMARY KEY,
                Number_of_Funds TEXT,
                Commitment TEXT,
                Percent_of_Total_Comm TEXT,
                Exposure TEXT,
                Percent_of_Total_Exposure TEXT,
                TVPI TEXT,
                Net_IRR TEXT
            )
        ''')
        print("Database table 'private_equity_firms' is ready.")

        # Insert or replace the data
        for firm in data:
            cursor.execute('''
                INSERT OR REPLACE INTO private_equity_firms 
                (Firm, Number_of_Funds, Commitment, Percent_of_Total_Comm, Exposure, Percent_of_Total_Exposure, TVPI, Net_IRR) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                firm.get('Firm', ''),
                firm.get('Number of Funds', ''),
                firm.get('Commitment', ''),
                firm.get('Percent of Total Comm', ''),
                firm.get('Exposure (FMV + Unfunded)', ''),
                firm.get('Percent of Total Exposure', ''),
                firm.get('TVPI', ''),
                firm.get('Net IRR', '')
            ))
            print(f"Inserted/Updated Firm: {firm.get('Firm', '')}")

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        return f"Successfully loaded {len(data)} records into the database."

    except Exception as e:
        return f"Error loading data into SQLite database: {str(e)}"

# Define the tool to process the pdf file
#@tool
def process_pdf_tool(file_path: str) -> str:
    """Process a PDF file and return a string."""
    results = process_pdf(file_path)
    print(results)
    return load_data(results)

#@tool
def display_table_tool() -> str:
    """Display the table."""
    return display_table()

print(process_pdf_tool(PDF_FILE_PATH))
print(display_table_tool())