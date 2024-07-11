from pydantic import BaseModel

class RegisterLocalFSSourceRequest(BaseModel):
    base_path: str
