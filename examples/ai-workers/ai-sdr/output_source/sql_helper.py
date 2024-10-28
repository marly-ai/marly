import sqlite3
from typing import Dict, List, Union
from tabulate import tabulate
import json

class SQLiteHelper:
    def __init__(self, db_path: str = 'contacts.db'):
        self.db_path = db_path
        self.create_tables()

    def create_tables(self):
        """Create the necessary tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            headline TEXT,
            location TEXT,
            summary TEXT,
            connections_count INTEGER
        )
        """)
        
        conn.commit()
        conn.close()

    def insert_contact(self, contact_data: Union[Dict, List[Dict]]) -> bool:
        """Insert one or more contacts into the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        insert_sql = """
        INSERT OR REPLACE INTO contacts (
            id, first_name, last_name, headline, location, summary, 
            connections_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Handle both single dict and list of dicts
            contacts_to_insert = contact_data if isinstance(contact_data, list) else [contact_data]
            
            for contact in contacts_to_insert:
                cursor.execute(insert_sql, (
                    contact['id'],
                    contact['first_name'],
                    contact['last_name'],
                    contact['headline'],
                    contact['location'],
                    contact['summary'],
                    contact['connections_count'],
                ))
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error inserting contact(s): {e}")
            return False
        finally:
            conn.close()

    def get_all_contacts(self) -> None:
        """Retrieve and display all contacts from the database in a formatted table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, first_name, last_name, headline, location, summary, connections_count FROM contacts")
        results = cursor.fetchall()
        
        conn.close()
        
        if results:
            keys = ['id', 'first_name', 'last_name', 'headline', 'location', 
                   'summary', 'connections_count']
            
            print(tabulate(results, 
                          headers=keys, 
                          tablefmt='grid',
                          maxcolwidths=[None, None, None, 30, 20, 40, None]))
            
            return [dict(zip(keys, result)) for result in results]
        
        print("No contacts found in database")
        return []
