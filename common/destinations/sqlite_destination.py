import sqlite3
import os
from typing import Dict, Any, List, Optional
from common.destinations.base.base_destination import BaseDestination

class SQLiteDestination(BaseDestination):
    def __init__(self, db_path: str, additional_params: Dict[str, Any] = None):
        self.db_path = db_path
        self.additional_params = additional_params or {}
        self.schema = self.additional_params.get("schema", "main")
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        if not self.conn:
            try:
                self.conn = sqlite3.connect(self.db_path)
                self.cursor = self.conn.cursor()
            except sqlite3.OperationalError:
                # If the directory doesn't exist, create it
                os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
                self.conn = sqlite3.connect(self.db_path)
                self.cursor = self.conn.cursor()
                print(f"Created new SQLite database at {self.db_path}")

    def insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        self.connect()
        if not data:
            raise ValueError("No data provided for insertion")

        if not self.table_exists(table_name):
            self.create_table(table_name, data[0].keys())

        columns = list(data[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        try:
            for row in data:
                values = [row.get(col, '') for col in columns]
                self.cursor.execute(insert_query, values)
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise ValueError(f"Error inserting data: {str(e)}")

    def table_exists(self, table_name: str) -> bool:
        self.connect()
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return self.cursor.fetchone() is not None

    def create_table(self, table_name: str, columns: List[str]) -> None:
        column_defs = ', '.join([f"{col} TEXT" for col in columns])
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
        self.cursor.execute(create_query)
        self.conn.commit()

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cursor = None

    def get_table_structure(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        self.connect()
        schema = schema or self.schema
        query = f"PRAGMA {schema}.table_info({table_name})"
        self.cursor.execute(query)
        columns = self.cursor.fetchall()

        structure = []
        for column in columns:
            structure.append({
                "name": column[1],
                "type": column[2],
                "notnull": bool(column[3]),
                "default_value": column[4],
                "pk": bool(column[5])
            })

        return structure
