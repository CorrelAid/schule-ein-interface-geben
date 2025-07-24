from anytree import NodeMixin
import polars as pl
from lib import BaseSchema

class DownloadCategoryNode(NodeMixin):
    def __init__(
        self,
        name,
        data_id=None,
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


class DownloadSchema(BaseSchema):
    fields = [
        {"name": "data_id", "type": pl.Int64, "nullable": False},
        {"name": "data_category_id", "type": pl.Int64, "nullable": False},
        {"name": "title", "type": pl.Utf8, "nullable": False},
        {"name": "category_title", "type": pl.Utf8, "nullable": False},
        {"name": "download_link", "type": pl.Utf8, "nullable": False},
        {"name": "file_type", "type": pl.Utf8, "nullable": False},
    ]


class SectionSchema(BaseSchema):
    fields = [
        {
            "name": "post_id",
            "type": pl.Int64,
            "nullable": False,
        },  # foreign key to posts (id)
        {"name": "title", "type": pl.Utf8, "nullable": True},
        {"name": "text", "type": pl.Utf8, "nullable": True},
        {
            "name": "type",
            "type": pl.Enum(
                [
                    "plain_text",
                    "accordion_section_text",
                    "accordion_section_prezi",
                    "accordion_section_youtube",
                    "accordion_section_h5p",
                    # "accordion_section_link",
                    "accordion_section_image",
                    "accordion_section_quiz",
                    "quiz",
                    "prezi",
                    "h5p",
                    "flipcard",
                    "image",
                    "video",
                ]
            ),
            "nullable": False,
        },
        {"name": "external_link", "type": pl.Utf8, "nullable": True},
    ]


class PostSchema(BaseSchema):
    fields = [
        {"name": "id", "type": pl.Int64, "nullable": False},
        {"name": "date", "type": pl.Date, "nullable": False},
        {"name": "title", "type": pl.Utf8, "nullable": False},
        {"name": "stage", "type": pl.Utf8, "nullable": True},
        {
            "name": "tool_types",
            "type": pl.List(
                pl.Enum(
                    [
                        "infografik",
                        "video",
                        "selbsttest",
                        "methode",
                        "vorlage",
                        "download",
                        "praesentation",
                    ]
                )
            ),
            "nullable": False,
        },
        {"name": "topics", "type": pl.List(pl.Utf8), "nullable": True},
        {"name": "download_chapter_dedicated", "type": pl.Int64, "nullable": True}, # dedicated download chapter if any (a full download chapter file tree is displayed in a posts download area or main section)
        {
            "name": "download_chapters_further", # links to download chapters (no file tree displayed) from the main area or further links area
            "type": pl.List(pl.Int64),
            "nullable": True,
        },
        {"name": "book_chapter", "type": pl.Utf8, "nullable": True},
        {"name": "related_posts", "type": pl.List(pl.Int64), "nullable": True}, # linked related posts from the further links or main area
    ]


class TermSchema(BaseSchema):
    fields = [
        {"name": "term", "type": pl.Utf8, "nullable": False},
        {"name": "definition", "type": pl.Utf8, "nullable": True},
        *[
            {"name": region, "type": pl.Utf8, "nullable": True}
            for region in [
                "DE",
                # ISO 3166-2 format for German states
                "DE_BW",
                "DE_BY",
                "DE_BE",
                "DE_BB",
                "DE_HB",
                "DE_HH",
                "DE_HE",
                "DE_MV",
                "DE_NI",
                "DE_NW",
                "DE_RP",
                "DE_SL",
                "DE_SN",
                "DE_ST",
                "DE_SH",
                "DE_TH",
            ]
        ],
    ]


# class Book(BaseModel):
#     id: int
#     title: str
#     subtitle: str
#     region_name: str
#     region_code: str = Field(
#         ..., pattern=r"^DE-[A-Z]{2}$"
#     )  # ISO 3166-2 format for German states
