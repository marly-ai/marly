from typing import Dict

class BaseSource:
    def connect(self):
        raise NotImplementedError

    def read(self, data: Dict):
        raise NotImplementedError

    def read_all(self):
        raise NotImplementedError