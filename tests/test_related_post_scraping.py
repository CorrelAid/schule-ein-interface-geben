
import requests
from lib.rendered_scraping import extract_related_posts_row

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
    result = extract_related_posts_row(row)
    return result


def test_related_post_in_text():
    # see also tests/test_post_main_scraping.py -> test_post_scraping_just_text
    post_id = 7022  # https://meinsvwissen.de/andere-schulformen/
    post_gt = [6848,4922]
    result = get_result(post_id)
    print(result)
    assert result == post_gt