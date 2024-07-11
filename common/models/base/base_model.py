from typing import Dict

class BaseModel:
    def do_completion(self, data: Dict):
        raise NotImplementedError