from typing import Dict

class BaseNormalization:
    def do_normalization(self, data: Dict):
        raise NotImplementedError
    
    def validate_output_format(self, data: Dict):
        raise NotImplementedError