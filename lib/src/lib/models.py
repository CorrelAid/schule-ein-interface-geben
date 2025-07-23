from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Annotated
from datetime import date
from anytree import NodeMixin


class DownloadCategoryNode(NodeMixin):
    def __init__(
        self,
        name,
        data_id=None,
        data_color=None,
        data_level=None,
        data_parent_id=None,
        parent=None,
        children=None,
    ):
        super(DownloadCategoryNode, self).__init__()
        self.name = name
        self.data_id = data_id
        self.data_level = data_level
        self.data_parent_id = data_parent_id
        self.parent = parent
        if children:
            self.children = children

    def __repr__(self):
        return f"DownloadCategoryNode(name='{self.name}', id={self.data_id})"


class Download(BaseModel):
    data_id: int
    data_category_id: int
    title: str
    category_title: str
    download_link: str
    file_type: str


class Section(BaseModel):
    title: Optional[str]
    text: Optional[
        str
    ]  # might be the result of transcription or extracting text from somewhere else
    type: Literal[
        "plain_text",
        "accordion_section_prezi",
        "accordion_section_youtube",
        "accordion_section_h5p",
        "accordion_section_link",
        "accordion_section_image",
        "accordion_section_quiz",
        "quiz",
        "prezi",
        "h5p",
        "flipcard",
        "image",
        "video",
    ]
    external_link: Optional[str]


class Post(BaseModel):
    id: int
    date: date
    title: str
    stage: Optional[Literal["grundlagen", "fortgeschrittene", "profis", "sv-alltag"]]
    tool_types: Annotated[
        List[
            Literal[
                "infografik",
                "video",
                "selbsttest",
                "methode",
                "vorlage",
                "download",
                "praesentation",
            ]
        ],
        Field(min_length=1),
    ]
    topics: Optional[List[str]]
    download_chapter_dedicated: Optional[
        int
    ]  # dedicated download chapter if any (a full download chapter file tree is displayed in a posts download area or main section)
    download_chapters_further: Optional[
        List[int]
    ]  # links to download chapters (no file tree displayed) from the main area or further links area
    book_chapter: Optional[str]
    related_posts: Optional[
        List[int]
    ]  # linked related posts from the further links area


class Term(BaseModel):
    term: str = Field(description="The glossary term itself.")
    definition: Optional[str] = Field(
        description="The general definition, insofar it does not relate to jurisdictions."
    )
    DE: Optional[str] = Field(
        None, description="Definition specific to the jurisdiction of Germany."
    )
    # ISO 3166-2 format for German states
    DE_BW: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Baden-Württemberg (DE-BW).",
    )
    DE_BY: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Bayern (DE-BY)."
    )
    DE_BE: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Berlin (DE-BE)."
    )
    DE_BB: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Brandenburg (DE-BB)."
    )
    DE_HB: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Bremen (DE-HB)."
    )
    DE_HH: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Hamburg (DE-HH)."
    )
    DE_HE: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Hessen (DE-HE)."
    )
    DE_MV: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Mecklenburg-Vorpommern (DE-MV).",
    )
    DE_NI: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Niedersachsen (DE-NI).",
    )
    DE_NW: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Nordrhein-Westfalen (DE-NW).",
    )
    DE_RP: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Rheinland-Pfalz (DE-RP).",
    )
    DE_SL: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Saarland (DE-SL)."
    )
    DE_SN: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Sachsen (DE-SN)."
    )
    DE_ST: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Sachsen-Anhalt (DE-ST).",
    )
    DE_SH: Optional[str] = Field(
        None,
        description="Definition specific to jurisdiction of Schleswig-Holstein (DE-SH).",
    )
    DE_TH: Optional[str] = Field(
        None, description="Definition specific to jurisdiction of Thüringen (DE-TH)."
    )


class Book(BaseModel):
    id: int
    title: str
    subtitle: str
    region_name: str
    region_code: str = Field(
        ..., pattern=r"^DE-[A-Z]{2}$"
    )  # ISO 3166-2 format for German states
