
import logging
import sys
import requests
from rich.logging import RichHandler
from lib.rendered_scraping import extract_related_posts_row, process_post_link

logging.basicConfig(
    level=logging.DEBUG,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=sys.stdout, rich_tracebacks=True)],
)
logger = logging.getLogger()  # root logger

def test_search():
    url = "https://meinsvwissen.de/das-abc-des-guten-teamgefuehls/"
    result = process_post_link(url, logger=logger)
    assert result == 4791
    url = "https://meinsvwissen.de/selbsttest-wie-bekannt-ist-unsere-sv/"
    result = process_post_link(url, logger=logger)
    assert result == 4791
    

def get_result(post_id):
    response = requests.get(f"https://meinsvwissen.de/wp-json/wp/v2/posts/{post_id}")
    response.raise_for_status()
    data = response.json()
    content = data["content"]["rendered"]
    title = data["title"]["rendered"]
    row = {
        "id": post_id,
        "content": content,
        "title": title,
    }
    result = extract_related_posts_row(row, logger=logger, max_workers=4)
    return result


def test_related_post_in_text():
    # see also tests/test_post_main_scraping.py -> test_post_scraping_just_text
    post_id = 7022  # https://meinsvwissen.de/andere-schulformen/
    post_gt = [6848, 4922]
    result = get_result(post_id)
    assert sorted(result) == sorted(post_gt)

def test_self_loop_fail():
    post_id = 4878  # https://meinsvwissen.de/nachhaltigkeit/
    post_gt = [3048, 4889, 5237, 5243]
    result = get_result(post_id)
    assert sorted(result) == sorted(post_gt)
