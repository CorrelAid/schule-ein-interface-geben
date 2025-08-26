
import requests
from lib.post_parsing import extract_book_chapter_row

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
    result = extract_book_chapter_row(row)
    return result

def test_get_book_chapter():
    post_id = 4746  # hhttps://meinsvwissen.de/oeffentlichkeitsarbeit-konzept/
    post_gt = "https://meinsvwissen.de/wp-content/uploads/2022/03/Öffentlichkeitsarbeit_S.122-131-.pdf"
    result = get_result(post_id)
    print(result)
    assert result == post_gt
