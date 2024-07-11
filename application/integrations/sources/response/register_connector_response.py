from pydantic import BaseModel

class RegisterSourceResponse(BaseModel):
    success: bool
    message: str