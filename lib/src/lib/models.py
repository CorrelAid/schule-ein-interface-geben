from pydantic import BaseModel, Field, field_validator, PlainValidator, ConfigDict, validator
from typing import Annotated, Literal, Optional
from lib.config import valid_jurisdictions

class Book(BaseModel):
    id: int
    title: str
    subtitle: str
    region_name: str
    region_code: str = Field(..., pattern=r"^DE-[A-Z]{2}$")  # ISO 3166-2 format for German states


class Jurisdiction_Extra(BaseModel):
    jurisdiction_name: str
    text: str
    
    @validator('jurisdiction_name')
    def check_jurisdiction(cls, v):
        if v not in list(valid_jurisdictions.values()):
            raise ValueError(f'Invalid jurisdiction: {v}')
        return v

class Term(BaseModel):
    term: str = Field(
        description="The glossary term itself.",
    )
    definition: Optional[str] = Field(
        description="The general definition, insofar it does not relate to jurisdictions.",
    )
    DE: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Germany."
    )
    DE_BW: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Baden-Württemberg (DE-BW)."
    )
    DE_BY: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Bayern (DE-BY)."
    )
    DE_BE: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Berlin (DE-BE)."
    )
    DE_BB: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Brandenburg (DE-BB)."
    )
    DE_HB: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Bremen (DE-HB)."
    )
    DE_HH: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Hamburg (DE-HH)."
    )
    DE_HE: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Hessen (DE-HE)."
    )
    DE_MV: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Mecklenburg-Vorpommern (DE-MV)."
    )
    DE_NI: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Niedersachsen (DE-NI)."
    )
    DE_NW: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Nordrhein-Westfalen (DE-NW)."
    )
    DE_RP: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Rheinland-Pfalz (DE-RP)."
    )
    DE_SL: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Saarland (DE-SL)."
    )
    DE_SN: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Sachsen (DE-SN)."
    )
    DE_ST: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Sachsen-Anhalt (DE-ST)."
    )
    DE_SH: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Schleswig-Holstein (DE-SH)."
    )
    DE_TH: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Thüringen (DE-TH)."
    )
