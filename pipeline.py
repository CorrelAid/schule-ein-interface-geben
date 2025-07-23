import dlt
import argparse
import random
import os
import concurrent.futures
import dspy
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import polars as pl
from rich.progress import Progress, TimeRemainingColumn, BarColumn, TextColumn
from anytree import RenderTree
from rich.logging import RichHandler
import logging

from lib.tree_functions import build_category_tree, get_node_lst, export_tree_to_json
from lib.custom_scraping_sources import (
    get_download_soup,
    get_file_links,
    extract_download_info,
)
from lib.transform import transform_api_results
from lib.dlt_stuff import api_source
from lib.config import valid_jurisdictions, tree_json_path
from lib.llm_parsers import make_termparser
from lib.config import llm_base_url, llm_model, db_name, pipeline_name
from lib.models import Term, Post, Download, Section

from lib.rendered_scraping import (
    process_posts_row,
    extract_further_download_category_ids,
    extract_book_chapter,
    extract_related_posts,
    extract_dedicated_download_chapter_id,
)


load_dotenv()

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)

log = logging.getLogger("rich")

SMOKE_TEST_N = 20
MAX_WORKERS = 4

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run the script with optional smoke test."
    )
    parser.add_argument(
        "--smoke-test", action="store_true", help="Run the script in smoke test mode"
    )
    return parser.parse_args()


args = parse_arguments()

if args.smoke_test:
    log.info("🚭 Smoke Test 🚭")

wp_user = os.getenv("WP_USER")
wp_pw = os.getenv("WP_PW")

assert wp_user and wp_pw, "WP_USER and WP_PW must be set in the environment variables"

lm = dspy.LM(
    model=f"openai/{llm_model}",
    model_type="chat",
    temperature=0.3,
    api_key=os.getenv("OR_KEY"),
    base_url=llm_base_url,
    cache=False,
)

pipeline = dlt.pipeline(
    pipeline_name=pipeline_name,
    destination="duckdb",
    dataset_name=db_name,
)

#######################################
#######################################

log.info(
    "[bold blue]🏭 Pipeline Stage 1/4: Get download files and their categories",
    extra={"markup": True},
)
log.info(
    "Get nested html list of all categories using selenium to log in to wp backend"
)
soup = get_download_soup(wp_user, wp_pw)

log.info("convert nested html list to tree structure")
root_node = build_category_tree(soup)

if args.smoke_test:
    log.debug("Displaying tree structure:")
    for pre, _, node in RenderTree(root_node):
        log.debug(
            f"{pre}{node.name} (ID: {node.data_id}, Level: {node.data_level}) {len(node.children) == 0}"
        )

log.info("Saving tree as json")

export_tree_to_json(root_node, tree_json_path)

category_ids = get_node_lst(root_node)

if args.smoke_test:
    category_ids = random.choices(category_ids, k=SMOKE_TEST_N)

log.info("go through all categories and get all file links")
file_link_lst = get_file_links(category_ids, max_workers=MAX_WORKERS)

log.info(
    "build a dataframe that contains available info on downloads, including dl url. We are also checking if the url works."
)
file_links = extract_download_info(file_link_lst, root_node, max_workers=MAX_WORKERS)

log.info(
    f"We extracted {len(file_links)} download urls from {len(category_ids)} categories"
)


@dlt.resource(table_name="downloads", columns=Download)
def downloads():
    yield from file_links.iter_rows(named=True)


load_info = pipeline.run(downloads, write_disposition="replace")

########################################
########################################

log.info("[bold blue]🏭 Pipeline Stage 2/4: Get posts", extra={"markup": True})
log.info("Requesting API")

if args.smoke_test:
    # limit yielded pages
    api_source.add_limit(1)

load_info = pipeline.run(api_source)
log.info("Transforming API results")
df_posts = transform_api_results(pipeline_name, db_name)

log.info(f"We extracted {len(df_posts)} posts.")

log.info("Extend posts with further download category")

df_posts_extended = extract_further_download_category_ids(df_posts)

log.info("Extend posts with book chapter")

df_posts_extended = extract_book_chapter(df_posts_extended)

log.info("Extend posts with related posts")

df_posts_extended = extract_related_posts(df_posts_extended)

log.info("Extend posts with dedicated download chapter id")

df_posts_extended = extract_dedicated_download_chapter_id(df_posts_extended)


@dlt.resource(table_name="posts_processed", columns=Post)
def posts_transformed():
    yield from df_posts_extended.iter_rows(named=True)


load_info = pipeline.run(posts_transformed, write_disposition="replace")

########################################
########################################

log.info("[bold blue]🏭 Pipeline Stage 3/4: Scrape Sections", extra={"markup": True})


def process_row(row):
    return process_posts_row(row)


sections = []
df_rows = list(df_posts.iter_rows(named=True))

if args.smoke_test:
    df_rows = random.choices(df_rows, k=SMOKE_TEST_N)

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_row, row) for row in df_rows]
    for future in concurrent.futures.as_completed(futures):
        temp = future.result()
        sections.extend(temp)

section_df = pl.DataFrame(sections)


@dlt.resource(table_name="sections", columns=Section)
def sections():
    yield from section_df.iter_rows(named=True)


load_info = pipeline.run(sections, write_disposition="replace")

########################################
########################################

log.info(
    "[bold blue]🏭 Pipeline Stage 4/4: Scraping Glossary and parsing terms",
    extra={"markup": True},
)


@dlt.resource(columns=Term)
def get_terms(smoke_test):
    term_parser = make_termparser()

    glossary_url = "https://meinsvwissen.de/glossar/"

    response = requests.get(glossary_url)
    soup = BeautifulSoup(response.content, "html.parser")

    elementors = soup.find_all(class_="elementor-toggle-item")
    if smoke_test:
        elementors = random.choices(elementors, k=SMOKE_TEST_N)
    dspy.configure(lm=lm)

    def parse_term(elem):
        title = elem.find(class_="elementor-toggle-title").get_text(strip=False)
        ps = elem.find_all("p")
        ps_text = ""
        for p in ps:
            p_text = p.get_text(strip=False)
            ps_text += f"{p_text}\n"
            term_dct = term_parser(
                valid_jurisdictions=valid_jurisdictions,
                raw_text=ps_text,
                term_input=title,
            )
        result = dict(term_dct)
        return result

    results = []
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Parsing terms...", total=len(elementors))
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_term = {
                executor.submit(parse_term, elem): elem for elem in elementors
            }
            for future in concurrent.futures.as_completed(future_to_term):
                progress.update(task, advance=1)
                result = future.result()
                results.append(result)
    yield results


# avoid info logging of litellm etc.
logging.getLogger().setLevel(logging.WARN)
load_info = pipeline.run(
    get_terms(smoke_test=args.smoke_test),
    write_disposition="replace",
    table_name="glossary_terms",
)
logging.getLogger().setLevel(logging.INFO)

log.info("[bold blue]🎉🎉🎉 We are done 🎉🎉🎉", extra={"markup": True})
