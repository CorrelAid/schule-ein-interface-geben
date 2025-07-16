import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator



source = rest_api_source({
   "client": {
                "base_url": "https://meinsvwissen.de/wp-json/wp/v2/",
            },
            "resource_defaults": {
                "endpoint": {
                    "paginator": {"type":"header_link", "links_next_key": "link"},
                    "params": {
                        "per_page": 100,
                    },
                },
                "write_disposition": "replace",
                
            },
            "resources": [
                "posts", # Beiträge (contains full content as html, including )
                "categories", # Toolart, e.g. Download -> many posts to many categories 
                "tags", # Themen, e.g. Anträge -> many posts to many categories 
                "stufe", # Stufe e.g. "Für Profis" -> many posts to one
            ],
        })

pipeline = dlt.pipeline(
    pipeline_name="segg",
    destination="duckdb",
    dataset_name="segg_data",
)

load_info = pipeline.run(source)