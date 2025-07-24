from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request
from typing import Any, List, Optional
from dlt.sources.rest_api.config_setup import register_paginator

class CustomPaginator(BasePaginator):
    def __init__(self, page_param: str = "page", initial_page: int = 1):
        super().__init__()
        self.page_param = page_param
        self.page = initial_page

    def init_request(self, request: Request) -> None:
        self.update_request(request)

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        self.max_page = int(response.headers.get("X-WP-TotalPages"))
        if self.max_page and self.page >= self.max_page:
            self._has_next_page = False
        else:
            self.page += 1

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}

        request.params[self.page_param] = self.page

register_paginator("custom_paginator", CustomPaginator)

api_source = rest_api_source(
    {
        "client": {
            "base_url": "https://meinsvwissen.de/wp-json/wp/v2/",
        },
        "resource_defaults": {
            "endpoint": {
                "paginator": {"type": "custom_paginator"},
                "params": {
                    "per_page": 20,
                },
            },
            "write_disposition": "replace",
        },
        "resources": [
            {"name": "posts_pre", "endpoint": {"path": "posts"}},  # Beiträge (contains full content as html, including )
            "categories",  # Toolart, e.g. Download -> many posts to many categories
            "tags",  # Themen, e.g. Anträge -> many posts to many categories
            "stufe",  # Stufe e.g. "Für Profis" -> many posts to one
        ],
    }
)