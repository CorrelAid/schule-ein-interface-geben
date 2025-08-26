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
    scrape_scc
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
    SCCSchema

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

table_names = [
    "student_council_committees"
    "legal_resources",
    "publications",
    "posts",
    "sections",
    "glossary_terms",
    "downloads",
]
#######################################
#######################################

step = 1
log.info(
    f"[bold blue]🏭 Pipeline Stage {step}/{len(table_names)}:  Get Student council committee info",
    extra={"markup": True},
)

scc_df = scrape_scc()

log.info(f"We got {len(scc_df)} legal councils")

step += 1


#######################################
#######################################

log.info(
    f"[bold blue]🏭 Pipeline Stage {step}/{len(table_names)}: Get legal_resources from jurisdiction as html",
    extra={"markup": True},
)

path = "static_data/legal_resources.json"

with open(path) as f:
    cfg = json.load(f)

debug_legal = False
if args.smoke_test:
    cfg = random.choices(cfg, k=SMOKE_TEST_N)
    debug_legal = True

legal_df = get_legal_resources(cfg, debug=debug_legal, logger=log)

log.info(f"We got {len(legal_df)} legal resources")
step += 1

#######################################
#######################################

log.info(
    f"[bold blue]🏭 Pipeline Stage {step}/{len(table_names)}: Getting publications from zotero",
    extra={"markup": True},
)
sample_k = -1
if args.smoke_test:
     sample_k = SMOKE_TEST_N

zotero_api_data = get_zotero_api_data(sample_k=SMOKE_TEST_N)

zotero_df = convert_zotero_api_results(zotero_api_data,  logger=log)

zotero_df = zotero_df.cast(PublicationSchema.to_polars_schema())

log.info(f"We got {len(zotero_df)} publications from zotero")
step += 1

#######################################
#######################################

log.info(
    f"[bold blue]🏭 Pipeline Stage {step}/{len(table_names)}: Get download files and their categories",
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

category_ids = get_node_lst(root_node)

if args.smoke_test:
    category_ids = random.choices(category_ids, k=SMOKE_TEST_N)

log.info("go through all categories and get all file links")
file_link_lst = get_file_links(category_ids, max_workers=MAX_WORKERS)

log.info(
    "build a dataframe that contains available info on downloads, including dl url. We are also checking if the url works."
)
downloads_df = extract_download_info(file_link_lst, root_node, max_workers=MAX_WORKERS)

downloads_df = downloads_df.cast(DownloadSchema.to_polars_schema())

log.info(
    f"We extracted {len(downloads_df)} download urls from {len(category_ids)} categories"
)
step += 1
########################################
########################################

log.info(
    f"[bold blue]🏭 Pipeline Stage {step}/{len(table_names)}: Get posts",
    extra={"markup": True},
)
log.info("Requesting API")

if args.smoke_test:
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

df_posts_extended = extract_related_posts(
    df_posts_extended, logger=log, max_workers=MAX_WORKERS
)

log.info("Extend posts with dedicated download chapter id")

df_posts_extended = extract_dedicated_download_chapter_id(df_posts_extended, root_node)

df_posts_extended = df_posts_extended.cast(PostSchema.to_polars_schema())
step += 1
########################################
########################################

log.info(
    f"[bold blue]🏭 Pipeline Stage {step}/{len(table_names)}: Scrape Sections",
    extra={"markup": True},
)

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

# check that all sections have valid posts ids
sec_ids = section_df["post_id"].unique().to_list()
post_ids = df_posts_extended["id"].unique().to_list()
missing = set(sec_ids) - set(post_ids)
assert not missing, f"Orphan section post_ids: {missing}"

assert section_df["type"].is_null().sum() == 0, "Section type is null"
step += 1
########################################
########################################

log.info(
    f"[bold blue]🏭 Pipeline Stage  {step}/{len(table_names)} Scraping Glossary and parsing terms",
    extra={"markup": True},
)

# avoid info logging of litellm etc.
logging.getLogger().setLevel(logging.WARN)

term_df = get_terms(args.smoke_test, SMOKE_TEST_N, MAX_WORKERS * 6, lm)

term_df = term_df.cast(TermSchema.to_polars_schema())

logging.getLogger().setLevel(logging.INFO)
step += 1
########################################
########################################

log.info(
    "[bold blue]⬆️ Post Pipeline: Uploading data",
    extra={"markup": True},
)


dfs = [scc_df, legal_df, zotero_df, df_posts_extended, section_df, term_df, downloads_df]
schemas = [
    SCCSchema.to_pyarrow_schema(),
    LegalResourceSchema.to_pyarrow_schema(),
    PublicationSchema.to_pyarrow_schema(),
    PostSchema.to_pyarrow_schema(),
    SectionSchema.to_pyarrow_schema(),
    TermSchema.to_pyarrow_schema(),
    DownloadSchema.to_pyarrow_schema()
]

for idx, table_name in enumerate(table_names):
    log.info(f"Uploading {table_name} to S3")
    if args.smoke_test:
        local_file_path = f"smoke_test_{table_name}.parquet"
    else:
        local_file_path = f"{table_name}.parquet"

    table = dfs[idx].to_arrow().select(schemas[idx].names).cast(schemas[idx])
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


if args.smoke_test:
    tree_json_path = f"smoke_test_{tree_json_path}"

export_tree_to_json(root_node, tree_json_path)

log.info(f"Uploading {tree_json_path} to S3")
client.upload_file(
    tree_json_path,
    S3_BUCKET_NAME,
    tree_json_path,
    ExtraArgs={"ACL": "public-read"},
)
log.info(f"Uploaded {tree_json_path} to S3")
os.remove(tree_json_path)
log.info(f"Removed local file {tree_json_path}")


log.info("[bold blue]🎉🎉🎉 We are done 🎉🎉🎉", extra={"markup": True})
