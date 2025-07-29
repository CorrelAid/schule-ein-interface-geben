import polars as pl
from bs4 import BeautifulSoup
from markdownify import markdownify
import requests
import re
import concurrent.futures
from lib.tree_functions import find_node_by_id
from lib.config import download_subpage
from lib.models import SectionSchema
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import random
import threading

category_pattern = r"#\d+-(\d+)"  # get second number after root category id


def extract_further_download_category_ids(df):
    def func(row):
        lst = []
        soup = BeautifulSoup(row["content"], "html.parser")
        list_item_widgets = soup.find_all(class_="elementor-icon-list-items")
        for list_item_widget in list_item_widgets:
            links = list_item_widget.find_all("a")
            further_dl_links = [
                a for a in links if a.has_attr("href") and download_subpage in a["href"]
            ]
            for link in further_dl_links:
                second_numbers = int(re.search(category_pattern, link["href"]).group(1))
                lst.append(second_numbers)
        toggle_items = soup.find_all(class_="elementor-toggle-item")
        for toggle_item in toggle_items:
            links = toggle_item.find_all("a")
            further_dl_links = [
                a for a in links if a.has_attr("href") and download_subpage in a["href"]
            ]
            for link in further_dl_links:
                second_numbers = int(re.search(category_pattern, link["href"]).group(1))
                lst.append(second_numbers)
        return lst

    df_ = df.with_columns(
        pl.struct("content", "title")
        .map_elements(lambda row: func(row), return_dtype=pl.List(pl.Int32))
        .alias("download_chapters_further")
    )
    return df_


def extract_book_chapter_row(row):
    soup = BeautifulSoup(row["content"], "html.parser")
    buttom_widgets = soup.find_all(class_="elementor-widget-button")
    buttom_widgets = [
        widget for widget in buttom_widgets if "volltext" in widget.get_text().lower()
    ]
    if len(buttom_widgets) == 0:
        return None
    elif len(buttom_widgets) > 1:
        raise ValueError("More than one button widget found")
    else:
        buttom_widgets = buttom_widgets[0]
        link = buttom_widgets.find("a")["href"]
        return link


def extract_book_chapter(df):
    df_ = df.with_columns(
        pl.struct("content", "title")
        .map_elements(lambda row: extract_book_chapter_row(row), return_dtype=pl.Utf8)
        .alias("book_chapter")
    )
    return df_

cache_lock = threading.Lock()
post_id_cache = {}
session = requests.Session()


def fetch_post_id(search_term):
    with cache_lock:
        if search_term in post_id_cache:
            return post_id_cache[search_term]

    url = (
        f"https://meinsvwissen.de/wp-json/wp/v2/search"
        f"?search={search_term}&type=post"
    )
    resp = session.get(url, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError(f"No posts found for '{search_term}'")

    post_id = data[0]["id"]
    with cache_lock:
        post_id_cache[search_term] = post_id
    return post_id


def process_post_link(href, logger, max_retries=3):
    backoff = 1
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(href, timeout=5)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            title_tag = soup.find("h1", class_="gb-headline")
            if not title_tag or not title_tag.text.strip():
                raise ValueError("Missing or empty title")

            search_term = title_tag.text.strip().replace("–", "&#8211;")
            logger.debug(f"Searching for post ID for '{search_term}'")
            return fetch_post_id(search_term)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else None
            logger.debug(f"Attempt {attempt}/{max_retries} HTTPError {status} for {href}")

            # immediately bail on 404
            if status == 404:
                logger.warning(f"404 Not Found: {href}")
                return None

        except Exception as e:
            logger.debug(f"Attempt {attempt}/{max_retries} failed for {href}: {e}")

        # Exponential backoff for non-404s
        if attempt < max_retries:
            time.sleep(backoff)
            backoff *= 2
        else:
            logger.error(f"Giving up on {href} after {max_retries} attempts")
            raise


def extract_related_posts_row(row, logger, max_workers):
    lst = []
    to_check = []
    soup = BeautifulSoup(row["content"], "html.parser")

    # icon list links
    for item in soup.find_all(class_="elementor-icon-list-item"):
        a = item.find("a", href=True)
        href = a["href"] if a else ""
        if "meinsvwissen" in href and download_subpage not in href:
            to_check.append(href)

    # embedded posts
    for post in soup.find_all("div", class_="wp-embed type-post"):
        href = post.find("a", class_="wp-embed-more")["href"]
        to_check.append(href)

    # standalone text links
    for widget in soup.find_all("div", class_="elementor-widget-text-editor"):
        for a in widget.find_all("a", href=True):
            href = a["href"]
            if (
                "meinsvwissen" in href
                and download_subpage not in href
                and "upload" not in href
                and "download" not in href
            ):
                to_check.append(href)

    # resolve links in parallel, skipping 404s
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_post_link, href, logger): href
                   for href in to_check}
        for future in concurrent.futures.as_completed(futures):
            href = futures[future]
            try:
                post_id = future.result()
                if post_id is not None:
                    lst.append(post_id)

            except requests.exceptions.HTTPError as e:
                # this only fires for non-404 HTTP errors after retries
                status = e.response.status_code if e.response else None
                if status == 404:
                    logger.warning(f"404 Not Found, skipping: {href}")
                    continue
                raise  # re-raise other HTTP errors

            except Exception as e:
                logger.error(f"Error processing {href}: {e}")
                raise

    return lst


def extract_related_posts(df, logger, max_workers):
    return df.with_columns(
        pl.struct("content", "title")
          .map_elements(
              lambda row: extract_related_posts_row(row, logger, max_workers),
              return_dtype=pl.List(pl.Int32),
          )
          .alias("related_posts")
    )


def extract_dedicated_download_chapter_id_row(row, root_node):
    soup = BeautifulSoup(row["content"], "html.parser")
    downloads = soup.find_all(class_="wpfd-tree-categories-files")

    if len(downloads) == 0:
        return None
    if len(downloads) > 1:
        raise ValueError("More than one download section found")

    firsta = downloads[0].find(class_="wpfd-file-link")
    # if there are file links displayed already, get the parent download category
    if firsta:
        if firsta.has_attr("data-category_id"):
            category_id = firsta["data-category_id"]
            return int(category_id)
        else:
            raise ValueError("No category_id found")
    # if there are no file links displayed, get the parent download category from the download tree
    else:
        firsta = downloads[0].find("a")
        if firsta:
            if firsta.has_attr("data-idcat"):
                subcat = firsta["data-idcat"]
                parent_node_id = find_node_by_id(root_node, subcat).data_parent_id
                return int(parent_node_id)
        else:
            raise ValueError(f"No download link found for {row['title']}")


def extract_dedicated_download_chapter_id(df, root_node):
    df_ = df.with_columns(
        pl.struct("content", "title")
        .map_elements(
            lambda row: extract_dedicated_download_chapter_id_row(row, root_node),
            return_dtype=pl.Int64,
        )
        .alias("download_chapter_dedicated")
    )
    return df_


def get_prezi_transcript_with_retry(url, logger, max_retries=3):
    for attempt in range(max_retries):
        try:
            return get_prezi_transcript(url, logger)
        except Exception as e:
            if attempt == max_retries - 1:
                logger.debug(f"Failed to fetch {url} after {max_retries} attempts")
                raise e
            time.sleep(1 * (attempt + 2))


prezi_pattern = r"https://prezi.com/p/embed\/(.+?)(?:\/|$)"


def get_prezi_transcript(url, logger):
    match = re.search(prezi_pattern, url)
    if not match:
        raise ValueError("Invalid Prezi URL")

    prezi_id = match.group(1)
    prezi_url = f"https://prezi.com/p/{prezi_id}/"

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")

    driver = None
    driver = webdriver.Chrome(options=options)
    driver.get(prezi_url)
    driver.implicitly_wait(4)

    if "Page not found" in driver.title:
        logger.warning(f"Page not found for URL: {url}")
        return None

    transcript_div = driver.find_element(by=By.ID, value="transcript-full-text")
    soup = BeautifulSoup(transcript_div.get_attribute("innerHTML"), "html.parser")
    text = str(soup)
    driver.quit()
    return text


def process_widget(widget, post_title, post_id, logger):
    sections = []
    if "elementor-widget-text-editor" in widget["class"]:
        text = markdownify(str(widget)).replace("\n", "")
        logger.debug("Appending section type: plain_text")
        sections.append(
            {
                "type": "plain_text",
                "text": text,
                "post_id": post_id,
            }
        )
        return sections
    ### Accordion ###
    elif "elementor-widget-toggle" in widget["class"]:
        toggle_sections = widget.find_all("div", class_="elementor-toggle-item")

        for toggle_section in toggle_sections:
            title = toggle_section.find("a", class_="elementor-toggle-title").get_text()
            toggle_content = toggle_section.find("div", class_="elementor-tab-content")
            logger.debug(f"Accordion title: {title}")
            ## Handle text in toggle section ##
            # This often comes before the actual content as its description
            if toggle_content.find_all("elementor-widget-text-editor"):
                for text_editor in toggle_content.find_all(
                    "elementor-widget-text-editor"
                ):
                    text = markdownify(str(text_editor)).replace("\n", "")
                    logger.debug(
                        f"Appending section type: accordion_section_text for post '{post_title}' ({post_id}) and accordion section '{title}'"
                    )
                    sections.append(
                        {
                            "title": title,
                            "type": "accordion_section_text",
                            "text": text,
                            "post_id": post_id,
                        }
                    )
            elif toggle_content.find("p"):
                for p in toggle_content.find_all("p"):
                    text = p.get_text().strip()
                    if text != "" and text != "\xa0":
                        logger.debug(
                            f"Appending section type: accordion_section_text for post '{post_title}' ({post_id}) and accordion section '{title}'"
                        )
                        sections.append(
                            {
                                "title": title,
                                "type": "accordion_section_text",
                                "text": text,
                                "post_id": post_id,
                            }
                        )

            if toggle_content.find_all("iframe"):
                logger.debug(f"Found iframes in '{title}' of post '{post_title}'")
                for iframe in toggle_content.find_all("iframe"):
                    if iframe.has_attr("src"):
                        if "prezi" in iframe["src"]:
                            external_link = iframe["src"]
                            text = get_prezi_transcript_with_retry(
                                external_link, logger
                            )
                            logger.debug(
                                f"Appending section type: accordion_section_prezi for post '{post_title}' ({post_id}) and accordion section '{title}'"
                            )
                            sections.append(
                                {
                                    "title": title,
                                    "type": "accordion_section_prezi",
                                    "external_link": external_link,
                                    "text": text,
                                    "post_id": post_id,
                                }
                            )
                        elif "youtube" in iframe["src"]:
                            external_link = iframe["src"]
                            logger.debug(
                                f"Appending section type: accordion_section_youtube for post '{post_title}' ({post_id}) and accordion section '{title}'"
                            )
                            sections.append(
                                {
                                    "title": iframe["title"],
                                    "type": "accordion_section_youtube",
                                    "external_link": external_link,
                                    "post_id": post_id,
                                }
                            )
                    elif iframe.has_attr("class"):
                        if "h5p-iframe" in iframe["class"]:
                            logger.debug(
                                f"Appending section type: accordion_section_h5p for post '{post_title}' ({post_id}) and accordion section '{title}'"
                            )
                            sections.append(
                                {
                                    "title": iframe["title"],
                                    "type": "accordion_section_h5p",
                                    "post_id": post_id,
                                }
                            )
                    else:
                        logger.debug(
                            f"Unknown Accordion iframe {title} {post_title}, {post_id}"
                        )
            elif toggle_content.find("img"):
                logger.debug(f"Found images in '{title}' of post '{post_title}'")
                for img in toggle_content.find_all("img"):
                    logger.debug(
                        f"Appending section type: accordion_section_image for post '{post_title}' ({post_id}) and accordion section '{title}'"
                    )
                    sections.append(
                        {
                            "title": title,
                            "type": "accordion_section_image",
                            "external_link": img["src"],
                            "post_id": post_id,
                        }
                    )

            elif toggle_content.find(class_="qsm-before-message"):
                quiz_message = toggle_content.find(
                    class_="qsm-before-message"
                ).get_text()
                logger.debug(
                    f"Appending section type: accordion_section_quiz for post '{post_title}' ({post_id}) and accordion section '{title}'"
                )
                sections.append(
                    {
                        "title": title,
                        "type": "accordion_section_quiz",
                        "text": quiz_message,
                        "post_id": post_id,
                    }
                )
            elif toggle_content.find("div", class_="wp-embed type-post"):
                None
            else:
                logger.debug(
                    f"Unknown Accordion section {title}, {post_title}, {post_id}"
                )

        return sections
    elif "elementor-widget-shortcode" in widget["class"]:
        if widget.find(class_="qsm-before-message"):
            quiz_message = widget.find(class_="qsm-before-message").get_text()
            logger.debug(
                f"Appending section type: quiz for post '{post_title}' ({post_id})"
            )
            sections.append({"type": "quiz", "text": quiz_message, "post_id": post_id})
            return sections
        elif widget.find("iframe"):
            for iframe in widget.find_all("iframe"):
                if "prezi" in iframe["src"]:
                    external_link = iframe["src"]
                    text = get_prezi_transcript_with_retry(external_link, logger)
                    logger.debug(
                        f"Appending section type: prezi for post '{post_title}' ({post_id}) and external link '{external_link}'"
                    )
                    sections.append(
                        {
                            "type": "prezi",
                            "external_link": external_link,
                            "text": text,
                            "post_id": post_id,
                        }
                    )
                    continue
                elif iframe.has_attr("class"):
                    if "h5p-iframe" in iframe["class"]:
                        iframe = widget.find("iframe", class_="h5p-iframe")
                        logger.debug(
                            f"Appending section type: h5p for post '{post_title}' ({post_id})"
                        )
                        sections.append(
                            {
                                "type": "h5p",
                                "title": iframe["title"],
                                "post_id": post_id,
                            }
                        )
                        continue
                else:
                    logger.warning(f"Unknown Standalone iframe {post_title}, {post_id}")
            return sections
        else:
            logger.warning(f"Unknown Shortcode {post_title}, {post_id}")
    elif "elementor-widget-htmega-flipbox-addons" in widget["class"]:
        ### Flipcard ###
        front_text = widget.find(class_="front-container").get_text()

        back_text = widget.find(class_="back-container").get_text()
        section = {}
        section["text"] = front_text + "\n" + back_text
        section["type"] = "flipcard"
        section["title"] = front_text
        if widget.find(class_="flp-btn"):
            link = widget.find("a")["href"]
            section["external_link"] = link
        section["post_id"] = post_id
        logger.debug(
            f"Appending section type: flipcard for post '{post_title}' ({post_id})"
        )
        sections.append(section)
        return sections
    elif "elementor-widget-image" in widget["class"]:
        src = widget.find("img")["src"]
        logger.debug(
            f"Appending section type: image for post '{post_title}' ({post_id})"
        )
        sections.append({"type": "image", "external_link": src, "post_id": post_id})
        return sections
    elif "elementor-video" in widget["class"]:
        src = widget.find("iframe")["src"]
        logger.debug(
            f"Appending section type: video for post '{post_title}' ({post_id})"
        )
        sections.append({"type": "video", "external_link": src, "post_id": post_id})
        return sections
    elif "elementor-widget-wpfd_choose_category":
        return None
    elif "elementor-widget-icon-list" in widget["class"]:
        return None
    elif "elementor-widget-spacer" in widget["class"]:
        return None
    elif "elementor-widget-heading" in widget["class"]:
        return None
    else:
        logger.warning(
            f"Unknown Widget {widget['class']} for post '{post_title}' ({post_id})"
        )
        return None


def extract_sections(row, logger):
    jitter = random.uniform(0.5, 1.5)
    time.sleep(jitter)
    soup = BeautifulSoup(row["content"], "html.parser")
    widgets = soup.find_all(class_="elementor-widget")
    widgets = [
        widget
        for widget in widgets
        if not widget.find_parent(class_="elementor-tab-content")
    ]

    sections = []
    for widget in widgets:
        result = process_widget(widget, row["title"], row["id"], logger)
        if result:
            if isinstance(result, list):
                sections.extend(result)
            else:
                sections.append(result)

    schema = SectionSchema.to_pydantic_model()
    for section in sections:
        schema.model_validate(section)
    return sections


def process_posts_row(row, logger):
    return extract_sections(row, logger)
