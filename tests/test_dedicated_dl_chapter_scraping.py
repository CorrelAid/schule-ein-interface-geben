
import requests
from lib.rendered_scraping import extract_dedicated_download_chapter_id_row
from lib.tree_functions import import_tree_from_json
from lib.config import tree_json_path

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
    root_node = import_tree_from_json(tree_json_path)
    result = extract_dedicated_download_chapter_id_row(row, root_node)
    return result


def test_materialsammlung():
    post_id = 7022  # https://meinsvwissen.de/andere-schulformen/
    post_gt = 42
    result = get_result(post_id)
    print(result)
    assert result == post_gt