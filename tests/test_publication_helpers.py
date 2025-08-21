from lib.pulication_helpers import get_zotero_api_data, convert_zotero_api_results
import polars as pl
import tempfile
from lib.models import PublicationSchema
import os
import pyarrow.parquet as pq
from rich.logging import RichHandler
import logging

def test_get_zotero_api_data():
    data = get_zotero_api_data()
    assert len(data) > 0
    assert isinstance(data, list)
    assert isinstance(data[0], dict)

def test_convert_zotero_api_results():
    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    log = logging.getLogger("rich")

    data = get_zotero_api_data(sample=False)
    df = convert_zotero_api_results(data[:10], logger=log)
    assert isinstance(df, pl.DataFrame)
    assert "pdf_binary" in df.columns

    pa_schema = PublicationSchema.to_pyarrow_schema()

    table = (
        df
        .to_arrow()
        .select(pa_schema.names)
        .cast(pa_schema)
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        local_file_path = os.path.join(tmpdir, "publications.parquet")
        pq.write_table(table, local_file_path)
        read_table = pq.read_table(local_file_path)

    assert read_table.schema == table.schema
    assert read_table.num_rows == table.num_rows
    assert read_table.column("key").to_pylist() == table.column("key").to_pylist()
    assert read_table.column("pdf_binary").to_pylist() == table.column("pdf_binary").to_pylist()
