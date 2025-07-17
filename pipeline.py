import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from lib.config import valid_jurisdictions
from lib.llm_parsers import make_termparser
from lib.config import llm_base_url, llm_model
from lib.models import Term
import requests
import concurrent.futures
import os
import dspy
from dotenv import load_dotenv
from bs4 import BeautifulSoup 

load_dotenv()

lm = dspy.LM(
    model=f"openai/{llm_model}",
    model_type="chat",
    temperature=0.3,
    api_key=os.getenv("OR_KEY"),
    base_url=llm_base_url,
    cache=False,
)

pipeline = dlt.pipeline(
    pipeline_name="segg",
    destination="duckdb",
    dataset_name="segg_data",
)

# source = rest_api_source({
#    "client": {
#                 "base_url": "https://meinsvwissen.de/wp-json/wp/v2/",
#             },
#             "resource_defaults": {
#                 "endpoint": {
#                     "paginator": {"type":"header_link", "links_next_key": "link"},
#                     "params": {
#                         "per_page": 100,
#                     },
#                 },
#                 "write_disposition": "replace",
                
#             },
#             "resources": [
#                 "posts", # Beiträge (contains full content as html, including )
#                 "categories", # Toolart, e.g. Download -> many posts to many categories 
#                 "tags", # Themen, e.g. Anträge -> many posts to many categories 
#                 "stufe", # Stufe e.g. "Für Profis" -> many posts to one
#             ],
#         })



# load_info = pipeline.run(source)

### Glossary


@dlt.resource(columns=Term)
def get_terms():
    term_parser = make_termparser()

    glossary_url = "https://meinsvwissen.de/glossar/"

    response = requests.get(glossary_url)
    soup = BeautifulSoup(response.content, "html.parser")

    elementors = soup.find_all(class_="elementor-toggle-item")
    dspy.configure(lm=lm)
    def parse_term(elem):
        title = elem.find(class_="elementor-toggle-title").get_text(strip=False)
        ps = elem.find_all("p")
        ps_text = ""
        for p in ps:
            p_text = p.get_text(strip=False)
            ps_text += f"{p_text}\n"
            term_dct = term_parser(valid_jurisdictions=valid_jurisdictions, raw_text=ps_text, term_input=title)
        result = dict(term_dct)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        terms = list(executor.map(parse_term, elementors))

    yield terms

load_info = pipeline.run(
    get_terms,
    write_disposition= "replace",
    table_name="glossary_terms",
)