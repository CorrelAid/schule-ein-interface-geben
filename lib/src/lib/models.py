from anytree import NodeMixin
from lib.config import valid_jurisdictions, valid_school_types
import polars as pl
from lib import BaseSchema

# The Downloads from https://meinsvwissen.de/sv-archiv/ as a Tree Structure
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

# Various files from https://meinsvwissen.de/sv-archiv/
class DownloadSchema(BaseSchema):
    fields = [
        {"name": "data_id", "type": pl.Int64, "nullable": False},
        {"name": "data_category_id", "type": pl.Int64, "nullable": False},
        {"name": "title", "type": pl.Utf8, "nullable": False},
        {"name": "category_title", "type": pl.Utf8, "nullable": False},
        {"name": "download_link", "type": pl.Utf8, "nullable": False},
        {"name": "file_type", "type": pl.Utf8, "nullable": False},
        {"name": "file_binary", "type": pl.Binary, "nullable": False},
    ]




# "Wissensmodule"/"Wissenskatalog"/Posts from https://meinsvwissen.de/wissen/
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
        {
            "name": "download_chapter_dedicated",
            "type": pl.Int64,
            "nullable": True,
        },  # dedicated download chapter if any (a full download chapter file tree is displayed in a posts download area or main section)
        {
            "name": "download_chapters_further",  # links to download chapters (no file tree displayed) from the main area or further links area
            "type": pl.List(pl.Int64),
            "nullable": True,
        },
        {"name": "book_chapter", "type": pl.Utf8, "nullable": True},
        {
            "name": "related_posts",
            "type": pl.List(pl.Int64),
            "nullable": True,
        },  # linked related posts from the further links or main area
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
                    "youtube",
                    "h5p",
                    "flipcard",
                    "image",
                    "video",
                ]
            ),
            "nullable": False,
        },
        {"name": "external_link", "type": pl.Utf8, "nullable": True},
        {"name": "transcript_url", "type": pl.Utf8, "nullable": True},
    ]

# Sub page of https://meinsvwissen.de/glossar-schuelervertretung/
class TermSchema(BaseSchema):
    fields = [
        {"name": "term", "type": pl.Utf8, "nullable": False},
        {"name": "definition", "type": pl.Utf8, "nullable": True},
        *[
            {"name": region, "type": pl.Utf8, "nullable": True}
            for region in list(valid_jurisdictions.keys())
        ],
    ]

# Publications from the Zotero group library: https://www.zotero.org/groups/6066861/segg/library
class PublicationSchema(BaseSchema):
    fields = [
        {"name": "key", "type": pl.Utf8, "nullable": False},
        {"name": "type", "type": pl.Utf8, "nullable": False},
        {"name": "title", "type": pl.Utf8, "nullable": False},
        {"name": "authors", "type": pl.List(pl.Utf8), "nullable": False},
        {"name": "abstract", "type": pl.Utf8, "nullable": True},
        {"name": "date", "type": pl.Utf8, "nullable": False},
        {"name": "url", "type": pl.Utf8, "nullable": False},
        {"name": "pdf_binary", "type": pl.Binary, "nullable": False},
        {
            "name": "jurisdiction",
            "type": pl.Enum(list(valid_jurisdictions.keys())),
            "nullable": True,
        },
        {
            "name": "school_type",
            "type": pl.Enum(list(valid_school_types.keys())),
            "nullable": True,
        },
        {"name": "tags", "type": pl.List(pl.Utf8), "nullable": True},
    ]


# Attempt to get the latest relevant legal sources for all german states 
class LegalResourceSchema(BaseSchema):
    fields = [
        {"name": "url", "type": pl.Utf8, "nullable": False},
        {"name": "type", "type": pl.Utf8, "nullable": False},
        {"name": "title", "type": pl.Utf8, "nullable": False},
        {"name": "html", "type": pl.Utf8, "nullable": False},
        {
            "name": "jurisdiction",
            "type": pl.Enum(list(valid_jurisdictions.keys())),
            "nullable": False,
        },
    ]

# https://www.bildungsserver.de/schule/gremien-der-schuelervertretung-sm-12681-de.html
class SCCSchema(BaseSchema):
    #Student council committees/Gremien der Schüler*innenvertretung
    fields = [
        {"name": "name", "type": pl.Utf8, "nullable": False},
        {"name": "description", "type": pl.Utf8, "nullable": False},
        {"name": "website", "type": pl.Utf8, "nullable": False},
                 {
            "name": "jurisdiction",
            "type": pl.Enum(list(valid_jurisdictions.keys())),
            "nullable": False,
        },
    ]

# https://svtipps.de
class SVTippsSchema(BaseSchema):
    fields = [
        {"name": "title", "type": pl.Utf8, "nullable": False},
        {"name": "url", "type": pl.Utf8, "nullable": False},
        {"name": "html_content", "type": pl.Utf8, "nullable": False},
        {"name": "category", "type": pl.Utf8, "nullable": True},  # Top level category (e.g. "Struktur", "Management")
        {"name": "subcategory", "type": pl.Utf8, "nullable": True},  # Subcategory if applicable
    ]
