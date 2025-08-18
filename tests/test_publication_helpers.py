from lib.pulication_helpers import get_zotero_api_data, convert_zotero_api_results
import polars as pl

def test_get_zotero_api_data():
    data = get_zotero_api_data()
    assert len(data) > 0
    assert isinstance(data, list)
    assert isinstance(data[0], dict)

def test_convert_zotero_api_results():
    data = get_zotero_api_data()
    converted_data = convert_zotero_api_results(data[:10])
    assert len(converted_data) > 0
    assert isinstance(converted_data, pl.DataFrame)