import polars as pl
from bs4 import BeautifulSoup 
import markdown
import requests
import re
import concurrent.futures
from lib.tree_functions import import_tree_from_json, find_node_by_id
from lib.config import tree_json_path, download_subpage
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

pattern = r'#\d+-(\d+)' # get second number after root category id

def extract_further_download_category_ids(df):
    def func(row):
        lst = []
        soup = BeautifulSoup(row["content"], "html.parser")
        list_item_widgets = soup.find_all(class_="elementor-icon-list-items")
        for list_item_widget in list_item_widgets:
            links = list_item_widget.find_all("a")
            further_dl_links = [a for a in links if a.has_attr("href") and download_subpage in a["href"] ] 
            for link in further_dl_links:
                second_numbers = int(re.search(pattern, link["href"]).group(1))
                lst.append(second_numbers)  
        toggle_items = soup.find_all(class_="elementor-toggle-item")
        for toggle_item in toggle_items:
            links = toggle_item.find_all("a")
            further_dl_links = [a for a in links if a.has_attr("href") and download_subpage in a["href"] ]
            for link in further_dl_links:
                second_numbers = int(re.search(pattern, link["href"]).group(1))
                lst.append(second_numbers)
        return lst
 
    df_ = df.with_columns(
        pl.struct('content','title').map_elements (lambda row: func(row), return_dtype=pl.List(pl.Int32)).alias("download_chapters_further")
    )
    return df_

def extract_book_chapter(df):
    def func(row):
        soup = BeautifulSoup(row["content"], "html.parser")
        buttom_widgets = soup.find_all(class_="elementor-widget-button")
        buttom_widgets = [widget for widget in buttom_widgets if "volltext" in widget.get_text().lower()]
        if len(buttom_widgets) == 0:
            return None
        elif len(buttom_widgets) > 1:
            raise ValueError("More than one button widget found")
        else:
            buttom_widgets = buttom_widgets[0]
            link = buttom_widgets.find("a")["href"]
            return link
        
 
    df_ = df.with_columns(
        pl.struct('content','title').map_elements (lambda row: func(row), return_dtype=pl.Utf8).alias("book_chapter")
    )
    return df_


def fetch_post_id(search_term):
    search_query = f"https://meinsvwissen.de/wp-json/wp/v2/search?search={search_term}&type=post"
    response = requests.get(search_query)
    if response.status_code == 200:
        return response.json()[0]["id"]
    else:
        raise Exception(f"Error: {response.status_code}")

def process_icon_item(href):
    match = re.search(pattern, href)
    if match:
        slug = match.group(1)
        search_term = slug.replace("-", " ")
        return fetch_post_id(search_term)
    return None

def extract_related_posts(df):
    def func(row):
        lst = []
        soup = BeautifulSoup(row["content"], "html.parser")
        icon_items = soup.find_all(class_="elementor-icon-list-item")

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for icon_item in icon_items:
                a = icon_item.find("a")
                if a is not None:
                    href = a["href"]
                    if "meinsvwissen" in href and download_subpage not in href:
                        futures.append(executor.submit(process_icon_item, href))

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    lst.append(result)
                    
        # posts linked in accordion
        posts = soup.find_all("div", class_="wp-embed type-post")
        for post in posts:
            href = post.find("a", class_="wp-embed-more")["href"]
            lst.append(fetch_post_id(href))
        return lst

    df_ = df.with_columns(
        pl.struct('content', 'title').map_elements(
            lambda row: func(row),
            return_dtype=pl.List(pl.Int32)
        ).alias("related_posts")
    )
    return df_

def extract_dedicated_download_chapter_id(df):
    root_node = import_tree_from_json(tree_json_path)
    def func(row):
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

    df_ = df.with_columns(
        pl.struct('content','title').map_elements (lambda row: func(row), return_dtype=pl.Int64).alias("download_chapter_dedicated")
    )
    return df_

prezi_pattern = r"https://prezi.com/p/embed\/(.+?)(?:\/|$)"

def get_prezi_transcript_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            return get_prezi_transcript(url)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts")
                return None
            time.sleep(1 * (attempt + 1))  

def get_prezi_transcript(url):
    match = re.search(prezi_pattern, url)
    if not match:
        raise ValueError("Invalid Prezi URL")

    prezi_id = match.group(1)
    prezi_url = f"https://prezi.com/p/{prezi_id}/"

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')

    driver = None
    driver = webdriver.Chrome(options=options)
    driver.get(prezi_url)
    driver.implicitly_wait(3)

    if "Page not found" in driver.title:
        print(f"Page not found for URL: {url}")
        return None

    transcript_div = driver.find_element(by=By.ID, value="transcript-full-text")
    soup = BeautifulSoup(transcript_div.get_attribute("innerHTML"), "html.parser")
    text = markdown.markdown(str(soup)).replace("\n", "")
    driver.quit()
    return text


def process_widget(widget, post_title,id):
    sections = []
    if "elementor-widget-text-editor" in widget["class"]:
        text = markdown.markdown(str(widget)).replace("\n", "")
        sections.append({
            "title": post_title,
            "type": "plain_text",
            "text": text
            })
        return sections
    ### Accordion ###
    elif "elementor-widget-toggle" in widget["class"]:
        toggle_sections = widget.find_all("div", class_="elementor-toggle-item")
        
        for toggle_section in toggle_sections:
            title = toggle_section.find("a", class_="elementor-toggle-title").get_text()
            if toggle_section.find("iframe"):
                # there can be multiple iframes in one toggle section
                for iframe in toggle_section.find_all("iframe"):
                    if iframe.has_attr("src"):
                        if "prezi" in iframe["src"]:
                            external_link = iframe["src"]
                            text = get_prezi_transcript_with_retry(external_link)
                            sections.append({
                                "title": title,
                                "type": "accordion_section_prezi",
                                "external_link": external_link,
                                "text": text
                            })
                            continue
                        elif "youtube" in iframe["src"]:
                            external_link = iframe["src"]
                            sections.append({
                                "title": title,
                                "type": "accordion_section_youtube",
                                "external_link": external_link
                            })
                            continue
                    elif iframe.has_attr("class"):
                        if "h5p-iframe" in iframe["class"]:
                            external_link = iframe["src"]
                            sections.append({
                                "title": title,
                                "type": "accordion_section_h5p",
                                "external_link": external_link
                                })
                            continue
                    else:
                        print(f"!! Unknown Accordion iframe {title} {post_title}, {id}")    
                return sections
            elif widget.find("a"):
                for link in widget.find_all("a"):
                    # added elsewhere
                    if link.has_attr("href"):
                        if download_subpage in link["href"]:
                            continue
                        elif "https://meinsvwissen.de/download/" in link["href"]:
                            sections.append({
                                "title": title,
                                "type": "accordion_section_link",
                                "external_link": link["href"]
                                })
                return sections
            elif widget.find_all("img"):
                for img in widget.find_all("img"):
                    sections.append({
                        "title": title,
                        "type": "accordion_section_image",
                        "external_link": img["src"]
                        })
                    continue
            elif widget.find(class_="qsm-before-message"):
                quiz_message = widget.find(class_="qsm-before-message").get_text()
                sections.append({
                    "title": title,
                    "type": "accordion_section_quiz",
                    "text": quiz_message
                    })
                continue
            # a linked post (handled elsewhere)
            elif toggle_section.find("div", class_="wp-embed type-post"):
                continue    
            else:
                print(f"!! Unknown Accordion section {title}, {post_title}, {id}")
            
        return sections
    elif "elementor-widget-shortcode" in widget["class"]:
        if widget.find(class_="qsm-before-message"):
            quiz_message = widget.find(class_="qsm-before-message").get_text()
            sections.append({
                "type": "quiz",
                "text": quiz_message
            })
            return sections
        elif widget.find("iframe"):
            for iframe in widget.find_all("iframe"):
                if "prezi" in iframe["src"]:
                    external_link = iframe["src"]
                    text = get_prezi_transcript_with_retry(external_link)
                    sections.append({
                        "type": "prezi",
                        "external_link": external_link,
                        "text": text
                    })
                    continue
                elif iframe.has_attr("class"):
                    if "h5p-iframe" in iframe["class"]:
                        iframe = widget.find("iframe", class_="h5p-iframe")
                        sections.append({
                            "type": "h5p",
                            "external_link": iframe["src"],
                            "title": iframe["title"]
                        })
                        continue
                else:
                    print(f"unknown Standalone iframe {post_title}, {id}")
            return sections
        else:
            print(f"!! Unknown Shortcode {post_title}, {id}")
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
        sections.append(section)
        return sections
    elif "elementor-widget-image" in widget["class"]:
        src =  widget.find("img")["src"]
        sections.append({
            "type": "image",
            "external_link": src
            })
        return sections
    elif "elementor-video" in widget["class"]:
        src =  widget.find("iframe")["src"]
        sections.append({
            "type": "video",
            "external_link": src
            })
        return sections
    ### Dedicated Download Category (either main section or in further links area) handled separately ###
    elif "elementor-widget-wpfd_choose_category":
        return None
    ### Further downloads (either main section or in further links area) handled separately ###
    elif "elementor-widget-icon-list" in widget["class"]:
        return None
    ### Spacer ###
    elif "elementor-widget-spacer" in widget["class"]:
        return None
    ### Heading ###
    elif "elementor-widget-heading" in widget["class"]:
        return None
    else:
        print("Unknown Widget", widget["class"])
        return None

def extract_sections(row):
    soup = BeautifulSoup(row["content"], "html.parser")
    widgets = soup.find_all(class_="elementor-widget")

    sections = []
    for widget in widgets:
        result = process_widget(widget,row["title"], row["id"])
        if result:
            if isinstance(result, list):
                sections.extend(result)
            else:
                sections.append(result)

    return sections

def process_posts_row(row):
    return extract_sections(row)
