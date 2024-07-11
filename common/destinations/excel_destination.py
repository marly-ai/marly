from typing import Dict, List
from application.integrations.destinations.repositories.destination_repository import DestinationRepository
import pandas as pd
import openpyxl
import uuid
from io import BytesIO

class ExcelIntegration:
    def __init__(self, data_source, output_filename):
        self.data_source = data_source
        self.output_filename = output_filename
        self.file_stream = self.get_file_stream(data_source, output_filename)
    
    def connect(self):
        pass

    def read(self, data: Dict) -> pd.DataFrame:
        sheet_name = data.get('sheet_name')
        start_cell = data.get('start_cell')
        return self.read_range(sheet_name, start_cell)

    def write(self, data: List[Dict]) -> str:
        try:
                   
            self.file_stream.seek(0)
            wb = openpyxl.load_workbook(self.file_stream, read_only=False, keep_vba=False, data_only=False, keep_links=False)
             
            sheet_cache = {sheet.title: sheet for sheet in wb.worksheets}
            
            for entry in data:
                data_location = entry.get('data_location')
                start_cell = entry.get('start_cell')
                values = entry.get('values')
                
                if data_location not in sheet_cache:
                    print(f"Sheet {data_location} not found, skipping entry.")
                    continue 
                
                ws = sheet_cache[data_location]
                
                start_column, start_row = openpyxl.utils.cell.coordinate_from_string(start_cell)
                start_column_index = openpyxl.utils.cell.column_index_from_string(start_column)
                start_row = int(start_row)
                
                for i, value in enumerate(values):
                    cell = ws.cell(row=start_row + i, column=start_column_index, value=value)
                    print(f"Inserting {value} into {cell.coordinate}")
            
            
            self.file_stream = BytesIO()
            wb.save(self.file_stream)
            self.file_stream.seek(0)
            
            filename = self.write_file_stream(data[0].get('output_filename'))
            return filename
        except Exception as e:
            return f"Error during write operation: {e}"

    def get_file_stream(self, data_source, output_filename):
        integration = DestinationRepository.get_integration(data_source)
        return integration.read({'file_key': output_filename})

    def read_range(self, sheet_name: str, start_cell: str) -> pd.DataFrame:
        df = pd.read_excel(self.file_stream, sheet_name=sheet_name)
        start_column, start_row = openpyxl.utils.cell.coordinate_from_string(start_cell)
        start_column_index = openpyxl.utils.cell.column_index_from_string(start_column) - 1
        return df.iloc[start_row-1:, start_column_index:]

    def write_file_stream(self, new_file_name: str):   
        new_file_name_with_uuid = f"{new_file_name.rsplit('.', 1)[0]}_{uuid.uuid4().hex[:5]}.xlsx"
        integration = DestinationRepository.get_integration(self.data_source)
        
        return integration.write({
            'file_name': new_file_name_with_uuid,
            'file_stream': self.file_stream.getvalue()
        })