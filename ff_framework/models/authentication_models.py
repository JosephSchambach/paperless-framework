from pydantic import BaseModel
from typing import Optional

class User(BaseModel):
    username: str
    password: str 
    email: str
    phone: str
    role: str
    
class UserAuthentication(BaseModel):
    username: str
    password: str
    organization_id: int
    
class UserRegistration(BaseModel):
    username: str
    password: str
    email: Optional[str] = None
    phone_number: Optional[str] = None
    role: Optional[str] = "admin"  
    organization_id: int

class CreateUserRegistration(BaseModel):
    user_registration: UserRegistration
    
class ProcessUserAuthentication(BaseModel):
    user_authentication: UserAuthentication