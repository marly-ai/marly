import logging
from typing import Dict, List
from common.normalization.base_normalization import BaseNormalization
from application.integrations.destinations.repositories.destination_repository import DestinationRepository
import openpyxl
from io import BytesIO

logger = logging.getLogger(__name__)

class ExcelNormalization(BaseNormalization):
    def __init__(self, output_data_source, output_filename):
        self.output_data_source = output_data_source
        self.output_filename = output_filename
        self.file_stream = self.get_file_stream()

    def do_normalization(self, data: List[Dict]):
        try:
            # Load the workbook once
            self.file_stream.seek(0)
            wb = openpyxl.load_workbook(self.file_stream, read_only=False, keep_vba=False, data_only=False, keep_links=False)
            
            # Cache worksheets to avoid repeated access
            sheet_cache = {sheet.title: sheet for sheet in wb.worksheets}
            
            for entry in data:
                data_location = entry.get('data_location')
                start_cell = entry.get('start_cell')
                values = entry.get('values')
                
                if data_location not in sheet_cache:
                    logger.info(f"Sheet {data_location} not found, skipping entry.")
                    continue
                
                ws = sheet_cache[data_location]
                
                start_column, start_row = openpyxl.utils.cell.coordinate_from_string(start_cell)
                start_column_index = openpyxl.utils.cell.column_index_from_string(start_column)
                start_row = int(start_row)
                
                for i, value in enumerate(values):
                    cell = ws.cell(row=start_row + i, column=start_column_index, value=value)
                    logger.info(f"Inserting {value} into {cell.coordinate}")
            
            # Save the workbook once after all insertions
            self.file_stream = BytesIO()
            wb.save(self.file_stream)
            self.file_stream.seek(0)
            return self.file_stream
        except Exception as e:
            logging.error(f"Error during normalization: {e}", exc_info=True)
            raise ValueError(f"Error during normalization: {e}")

    def get_file_stream(self):
        integration = DestinationRepository.get_integration(self.output_data_source)
        file_data = integration.read({'file_key': self.output_filename})
        if isinstance(file_data, BytesIO):
            return file_data
        return BytesIO(file_data)
