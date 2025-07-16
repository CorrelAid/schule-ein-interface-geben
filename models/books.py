from pydantic import BaseModel, Field

class Book(BaseModel):
    id: int
    title: str
    subtitle: str
    region_name: str
    region_code: str = Field(..., regex=r"^DE-[A-Z]{2}$")  # ISO 3166-2 format for German states
