import dlt
import argparse
import random
import os
import concurrent.futures
import dspy
import boto3
from botocore.config import Config
import json
from dotenv import load_dotenv
import polars as pl
from anytree import RenderTree
from rich.logging import RichHandler
import logging
from lib.tree_functions import build_category_tree, get_node_lst, export_tree_to_json
from lib.scraping import (
    get_download_soup,
    get_file_links,
    extract_download_info,
    get_terms,
    scrape_scc,
    scrape_svtipps
)
from lib.transform import transform_api_results
from lib.dlt_defs import api_source
from lib.config import tree_json_path, llm_base_url, llm_model, db_name, pipeline_name
from lib.models import (
    DownloadSchema,
    PostSchema,
    TermSchema,
    SectionSchema,
    PublicationSchema,
    LegalResourceSchema,
    SCCSchema,
    SVTippsSchema
)
from lib.post_parsing import (
    process_posts_row,
    extract_further_download_category_ids,
    extract_book_chapter,
    extract_related_posts,
    extract_dedicated_download_chapter_id,
)
from lib.legal_res_helpers import get_legal_resources
from lib.pulication_helpers import get_zotero_api_data, convert_zotero_api_results
import pyarrow.parquet as pq

load_dotenv()

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)

log = logging.getLogger("rich")

SMOKE_TEST_N = 3
MAX_WORKERS = 3
S3_BUCKET_NAME = "cdl-segg"

config = Config(
    region_name="fra1",
    connect_timeout=20,
    read_timeout=60,
    retries={"max_attempts": 8, "mode": "standard"},
    s3={"addressing_style": "path"},
)


session = boto3.session.Session()
client = session.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
    config=config,
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run the script with optional smoke test and step selection."
    )
    parser.add_argument(
        "--smoke-test", action="store_true", help="Run the script in smoke test mode"
    )
    parser.add_argument(
        "--steps",
        nargs="+",
        metavar="STEP_NAME",
        help="Run only specific steps. Available: student_council_committees, svtipps, legal_resources, publications, downloads_and_posts_and_sections, glossary_terms"
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

table_names = [
    "student_council_committees",
    "svtipps",
    "legal_resources",
    "publications",
    "downloads_and_posts_and_sections",  # Combined step due to dependencies
    "glossary_terms",
]

# Validate and setup step selection
if args.steps:
    invalid_steps = set(args.steps) - set(table_names)
    if invalid_steps:
        log.error(f"Invalid step names: {invalid_steps}")
        log.error(f"Available steps: {table_names}")
        exit(1)
    steps_to_run = set(args.steps)
    log.info(f"Running only selected steps: {sorted(steps_to_run)}")
else:
    steps_to_run = set(table_names)
    log.info("Running all pipeline steps")


# Upload function
def upload_to_s3(df, table_name, schema):
    """Upload dataframe to S3 as parquet"""
    if args.smoke_test:
        local_file_path = f"smoke_test_{table_name}.parquet"
    else:
        local_file_path = f"{table_name}.parquet"

    log.info(f"Uploading {table_name} to S3")

    arrow_table = df.to_arrow()
    df_columns = set(arrow_table.column_names)
    schema_columns = set(schema.names)

    missing_in_df = schema_columns - df_columns
    extra_in_df = df_columns - schema_columns

    if missing_in_df:
        raise ValueError(f"DataFrame missing required schema fields for {table_name}: {missing_in_df}")
    if extra_in_df:
        log.warning(f"Extra fields in DataFrame for {table_name}: {extra_in_df}")

    table = arrow_table.select(schema.names)

    # Cast table to match the expected schema types
    table = table.cast(schema)
    pq.write_table(table, local_file_path)

    client.upload_file(
        local_file_path,
        S3_BUCKET_NAME,
        local_file_path,
        ExtraArgs={"ACL": "public-read"},
    )
    log.info(f"Uploaded {table_name} to S3")
    os.remove(local_file_path)
    log.info(f"Removed local file {local_file_path}")


# Pipeline step functions
def step_student_council_committees():
    """Get Student council committee info"""
    log.info("🏭 Get Student council committee info")
    scc_df = scrape_scc()
    log.info(f"We got {len(scc_df)} councils")
    upload_to_s3(scc_df, "student_council_committees", SCCSchema.to_pyarrow_schema())
    return scc_df


def step_svtipps():
    """Get SV tipps"""
    log.info("🏭 Get SV tipps")
    sample_k = SMOKE_TEST_N if args.smoke_test else -1
    svtipps_df = scrape_svtipps(sample_k=sample_k)
    log.info(f"We got {len(svtipps_df)} SV tipps")
    upload_to_s3(svtipps_df, "svtipps", SVTippsSchema.to_pyarrow_schema())
    return svtipps_df


def step_legal_resources():
    """Get legal_resources from jurisdiction as html"""
    log.info("🏭 Get legal_resources from jurisdiction as html")
    path = "static_data/legal_resources.json"
    with open(path) as f:
        cfg = json.load(f)

    debug_legal = False
    if args.smoke_test:
        cfg = random.choices(cfg, k=SMOKE_TEST_N)
        debug_legal = True

    legal_df = get_legal_resources(cfg, debug=debug_legal, logger=log)
    log.info(f"We got {len(legal_df)} legal resources")
    upload_to_s3(legal_df, "legal_resources", LegalResourceSchema.to_pyarrow_schema())
    return legal_df


def step_publications():
    """Getting publications from zotero"""
    log.info("🏭 Getting publications from zotero")
    sample_k = SMOKE_TEST_N if args.smoke_test else -1
    zotero_api_data = get_zotero_api_data(sample_k=sample_k)
    zotero_df = convert_zotero_api_results(zotero_api_data, logger=log)
    zotero_df = zotero_df.cast(PublicationSchema.to_polars_schema())
    log.info(f"We got {len(zotero_df)} publications from zotero")
    upload_to_s3(zotero_df, "publications", PublicationSchema.to_pyarrow_schema())
    return zotero_df


def step_downloads_and_posts_and_sections():
    """Combined step: Get downloads, posts, and sections (due to dependencies)"""
    # Step 1: Downloads
    log.info("🏭 Get download files and their categories")
    log.info("Get nested html list of all categories using selenium to log in to wp backend")
    soup = get_download_soup(wp_user, wp_pw)

    log.info("convert nested html list to tree structure")
    root_node = build_category_tree(soup)

    if args.smoke_test:
        log.debug("Displaying tree structure:")
        for pre, _, node in RenderTree(root_node):
            log.debug(f"{pre}{node.name} (ID: {node.data_id}, Level: {node.data_level}) {len(node.children) == 0}")

    category_ids = get_node_lst(root_node)

    if args.smoke_test:
        category_ids = random.choices(category_ids, k=SMOKE_TEST_N)

    log.info("go through all categories and get all file links")
    file_link_lst = get_file_links(category_ids, max_workers=MAX_WORKERS)

    log.info("build a dataframe that contains available info on downloads, including dl url. We are also checking if the url works.")
    downloads_df = extract_download_info(file_link_lst, root_node, max_workers=MAX_WORKERS)
    downloads_df = downloads_df.cast(DownloadSchema.to_polars_schema())

    log.info(f"We extracted {len(downloads_df)} download urls from {len(category_ids)} categories")
    upload_to_s3(downloads_df, "downloads", DownloadSchema.to_pyarrow_schema())

    # Step 2: Posts
    log.info("🏭 Get posts")
    log.info("Requesting API")

    if args.smoke_test:
        api_source.add_limit(1)

    load_info = pipeline.run(api_source)
    log.info("Transforming API results")
    df_posts = transform_api_results(pipeline_name, db_name)
    log.info(f"We extracted {len(df_posts)} posts.")

    # Step 3: Extend posts (requires root_node from downloads)
    log.info("Extend posts with further download category")
    df_posts_extended = extract_further_download_category_ids(df_posts)

    log.info("Extend posts with book chapter")
    df_posts_extended = extract_book_chapter(df_posts_extended)

    log.info("Extend posts with related posts")
    df_posts_extended = extract_related_posts(df_posts_extended, logger=log, max_workers=MAX_WORKERS)

    log.info("Extend posts with dedicated download chapter id")
    df_posts_extended = extract_dedicated_download_chapter_id(df_posts_extended, root_node)
    df_posts_extended = df_posts_extended.cast(PostSchema.to_polars_schema())
    upload_to_s3(df_posts_extended, "posts", PostSchema.to_pyarrow_schema())

    # Step 4: Sections (requires df_posts)
    log.info("🏭 Scrape Sections")

    def process_row(row):
        return process_posts_row(row, logger=log)

    sections = []
    df_rows = list(df_posts.iter_rows(named=True))

    if args.smoke_test:
        df_rows = random.choices(df_rows, k=SMOKE_TEST_N)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_row, row) for row in df_rows]
        for future in concurrent.futures.as_completed(futures):
            temp = future.result()
            sections.extend(temp)

    section_df = pl.DataFrame(sections, schema_overrides=SectionSchema.to_polars_schema())

    # Validate sections against posts
    sec_ids = section_df["post_id"].unique().to_list()
    post_ids = df_posts_extended["id"].unique().to_list()
    missing = set(sec_ids) - set(post_ids)
    assert not missing, f"Orphan section post_ids: {missing}"
    assert section_df["type"].is_null().sum() == 0, "Section type is null"

    upload_to_s3(section_df, "sections", SectionSchema.to_pyarrow_schema())

    # Upload tree JSON
    if args.smoke_test:
        tree_json_filename = f"smoke_test_{tree_json_path}"
    else:
        tree_json_filename = tree_json_path

    export_tree_to_json(root_node, tree_json_filename)
    log.info(f"Uploading {tree_json_filename} to S3")
    client.upload_file(
        tree_json_filename,
        S3_BUCKET_NAME,
        tree_json_filename,
        ExtraArgs={"ACL": "public-read"},
    )
    log.info(f"Uploaded {tree_json_filename} to S3")
    os.remove(tree_json_filename)
    log.info(f"Removed local file {tree_json_filename}")

    return downloads_df, df_posts_extended, section_df


def step_glossary_terms():
    """Scraping Glossary and parsing terms"""
    log.info("🏭 Scraping Glossary and parsing terms")

    # avoid info logging of litellm etc.
    logging.getLogger().setLevel(logging.WARN)
    term_df = get_terms(args.smoke_test, SMOKE_TEST_N, MAX_WORKERS * 6, lm)
    term_df = term_df.cast(TermSchema.to_polars_schema())
    logging.getLogger().setLevel(logging.INFO)
    upload_to_s3(term_df, "glossary_terms", TermSchema.to_pyarrow_schema())
    return term_df
# Main pipeline execution
step_functions = {
    "student_council_committees": step_student_council_committees,
    "svtipps": step_svtipps,
    "legal_resources": step_legal_resources,
    "publications": step_publications,
    "downloads_and_posts_and_sections": step_downloads_and_posts_and_sections,
    "glossary_terms": step_glossary_terms,
}

# Execute selected steps
step_num = 1
total_steps = len([name for name in table_names if name in steps_to_run])

for table_name in table_names:
    if table_name in steps_to_run:
        log.info(f"[bold blue]🏭 Pipeline Stage {step_num}/{total_steps}: {table_name}", extra={"markup": True})
        results = step_functions[table_name]()
        step_num += 1
    else:
        log.info(f"[yellow]⏭️ Skipping: {table_name}", extra={"markup": True})

log.info("[bold blue]🎉🎉🎉 We are done 🎉🎉🎉", extra={"markup": True})

