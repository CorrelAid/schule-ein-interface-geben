from lib import BaseSchema
import pyarrow
import polars as pl

def test_typing():
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
            {"name": "download_chapter_dedicated", "type": pl.Int64, "nullable": True},
            {
                "name": "download_chapters_further",
                "type": pl.List(pl.Int64),
                "nullable": True,
            },
            {"name": "book_chapter", "type": pl.Utf8, "nullable": True},
            {"name": "related_posts", "type": pl.List(pl.Int64), "nullable": True},
        ]

    polars_schema = PostSchema.to_polars_schema()
    print(polars_schema)

    pyarrow_schema = PostSchema.to_pyarrow_schema()
    print(pyarrow_schema)

    pydantic_model = PostSchema.to_pydantic_model()
    print(pydantic_model)