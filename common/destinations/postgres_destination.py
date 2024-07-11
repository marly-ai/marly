from common.destinations.base_destination import BaseDestination
from typing import Dict, List
from psycopg2.extras import execute_batch
from psycopg2.pool import ThreadedConnectionPool
import logging
import json

logger = logging.getLogger(__name__)

class PostgresIntegration(BaseDestination):
    def __init__(self, host: str, port: int, user: str, password: str, database: str, schema: str = 'public'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.pool = None
        self.min_conn = 1
        self.max_conn = 10

    def connect(self):
        try:
            self.pool = ThreadedConnectionPool(
                self.min_conn,
                self.max_conn,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                options=f'-c search_path={self.schema}'
            )
            logger.info("Successfully connected to PostgreSQL database.")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def write(self, data: Dict):
        if not self.pool:
            raise ValueError("Not connected to PostgreSQL. Call connect() first.")

        location = data.get('data_location')
        values = data.get('transformed_metrics')
        
        if not location or not values:
            logger.error("Invalid data format. Both 'data_location' and 'transformed_metrics' are required.")
            return

        # Parse the JSON string into a Python dictionary
        try:
            values_dict = json.loads(values)
        except json.JSONDecodeError:
            logger.error("Error decoding JSON string in transformed_metrics")
            raise

        schema, table_name = self._parse_location(location)

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                self._batch_insert(cursor, table_name, values_dict, schema)
                conn.commit()
            logger.info(f"Successfully wrote data to {location}")
            return location
        except Exception as e:
            conn.rollback()
            logger.error(f"Error writing to PostgreSQL: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def _parse_location(self, location: str) -> tuple:
        parts = location.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            return 'public', parts[0]
        else:
            raise ValueError(f"Invalid location format: {location}")

    def _batch_insert(self, cursor, table_name: str, values: Dict, schema: str = 'public'):
        if not values:
            return
        
        columns = list(values.keys())
        placeholders = ', '.join(['%s' for _ in columns])
        
        query = f"""
            INSERT INTO "{schema}"."{table_name}" ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (firm) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'firm'])}
        """
        
        # Transpose the data: each row is now a list of values for one record
        data = list(zip(*values.values()))
        
        execute_batch(cursor, query, data, page_size=1000)

    def __del__(self):
        if hasattr(self, 'pool') and self.pool:
            self.pool.closeall()
            logger.info("Closed all database connections.")
