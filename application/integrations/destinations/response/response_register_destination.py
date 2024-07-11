from pydantic import BaseModel

class RegisterIntegrationResponse(BaseModel):
    success: bool
    message: str