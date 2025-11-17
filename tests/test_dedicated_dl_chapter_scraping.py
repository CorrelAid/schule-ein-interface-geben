import requests
from lib.post_parsing import extract_dedicated_download_chapter_id_row
from lib.scraping import download_file_binary, get_download_soup
from lib.tree_functions import build_category_tree
import os
from dotenv import load_dotenv

load_dotenv()


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
    wp_user = os.getenv("WP_USER")
    wp_pw = os.getenv("WP_PW")
    soup = get_download_soup(wp_user, wp_pw)
    root_node = build_category_tree(soup)
    result = extract_dedicated_download_chapter_id_row(row, root_node)
    return result


def test_materialsammlung():
    post_id = 7022  # https://meinsvwissen.de/andere-schulformen/
    post_gt = 42
    result = get_result(post_id)
    print(result)
    assert result == post_gt


def test_in_main_and_dedicated():
    post_id = 7327  # https://meinsvwissen.de/andere-schulformen/
    post_gt = 156
    result = get_result(post_id)
    print(result)
    assert result == post_gt


def test_memory_spiel_konferenzen():
    post_id = 8952  # https://meinsvwissen.de/memory-spiel-konferenzen-sachsen-anhalt/
    post_gt = 41
    result = get_result(post_id)
    print(f"Post {post_id}: Expected {post_gt}, Got {result}")
    assert result == post_gt


def test_download_file_binary():
    url = "https://httpbin.org/bytes/1024"
    success, file_type, content = download_file_binary(url)

    print(
        f"Download test - Success: {success}, File type: {file_type}, Content length: {len(content) if content else 0}"
    )

    assert success
    assert content is not None
    assert len(content) == 1024
    assert isinstance(content, bytes)


def test_load_file_from_binary():
    url = "https://httpbin.org/robots.txt"
    success, file_type, binary_content = download_file_binary(url)
    
    print(f"Binary download - Success: {success}, File type: {file_type}")
    assert success
    assert binary_content is not None
    assert isinstance(binary_content, bytes)
    
    text_content = binary_content.decode('utf-8')
    print(f"Loaded content preview: {text_content[:50]}...")
    
    assert "User-agent" in text_content
    
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(binary_content)
        temp_file_path = temp_file.name
    
    with open(temp_file_path, 'rb') as f:
        loaded_binary = f.read()

    os.unlink(temp_file_path)
    
    assert loaded_binary == binary_content

