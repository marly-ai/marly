from typing import Dict

class BaseDestination:
    def connect(self):
        raise NotImplementedError

    def write(self, data: Dict):
        raise NotImplementedError