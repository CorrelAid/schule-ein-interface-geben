from lib.tree_functions import find_node_by_id
import requests
from rich.progress import Progress, TimeRemainingColumn, BarColumn, TextColumn
import concurrent.futures
import polars as pl
from selenium import webdriver
import json
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from lib.llm_parsers import make_termparser
import random
from lib.config import valid_jurisdictions
import dspy
from lib.models import SCCSchema, SVTippsSchema
from html_sanitizer import Sanitizer


def download_file_binary(url, max_retries=5, retry_delay=3):
    jitter = random.uniform(2, 4)
    time.sleep(jitter)
    for attempt in range(max_retries):
        try:
            response = requests.get(url, allow_redirects=True, timeout=60, stream=True)
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if content_type:
                    # stream can be odt
                    file_type = content_type.split("/")[-1]
                else:
                    file_type = "unknown"
                return True, file_type, response.content
            else:
                return False, "unknown", None
        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            else:
                return False, "unknown", None


def process_link(link, root_node):
    category_id = int(link.get("data-category_id", 0))
    data_id = int(link.get("data-id", 0))
    title = link.get("title", "")
    category_title = find_node_by_id(root_node, str(category_id)).name
    download_link = f"https://meinsvwissen.de/download/{category_id}/so-weird-that/{data_id}/this-does-not-matter".lower()

    is_valid, file_type, file_binary = download_file_binary(download_link)

    if is_valid:
        return {
            "data_id": data_id,
            "data_category_id": category_id,
            "title": title,
            "category_title": category_title,
            "download_link": download_link,
            "file_type": file_type,
            "file_binary": file_binary,
        }
    else:
        raise Exception(f"Invalid link: {download_link}")


def extract_download_info(lst, root_node, max_workers=4):
    data = []

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Extracting download info...", total=len(lst))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_link = {
                executor.submit(process_link, link, root_node): link for link in lst
            }

            for future in concurrent.futures.as_completed(future_to_link):
                result = future.result()
                data.append(result)
                progress.update(task, advance=1)

    df = pl.DataFrame(data)
    return df


def get_download_soup(wp_user, wp_pw, max_retries=3):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(6)

    retry_count = 0
    try:
        while retry_count < max_retries:
            try:
                driver.get("https://meinsvwissen.de/wp-admin/admin.php?page=wpfd")
                time.sleep(10)
                user = driver.find_element(By.ID, "user_login")
                pw = driver.find_element(By.ID, "user_pass")
                submit_button = driver.find_element(By.ID, "wp-submit")

                user.clear()
                user.send_keys(wp_user)
                pw.clear()
                pw.send_keys(wp_pw)
                submit_button.click()

                # Wait for page to load after login
                time.sleep(2)

                file_list = driver.find_element(By.ID, "categorieslist")
                html = file_list.get_attribute("innerHTML")
                soup = BeautifulSoup(html, "html.parser")
                return soup

            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying... (Attempt {retry_count + 1}/{max_retries})")
                    # eponential backoff with jitter
                    time.sleep(2.5**retry_count + random.uniform(2, 4))
                else:
                    raise Exception(
                        f"Max retries ({max_retries}) reached in get_download_soup."
                    )
    finally:
        driver.quit()


def process_id(id, index, total, max_retries=5):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(5)

    retry_count = 0
    success = False
    html = None

    try:
        while retry_count < max_retries and not success:
            try:
                jitter = random.uniform(3, 5)
                time.sleep(jitter)
                driver.get(f"https://meinsvwissen.de/sv-archiv/#36-{id}")
                time.sleep(5)

                container = driver.find_element(By.ID, "wpfd-elementor-category")
                html = container.get_attribute("innerHTML")
                success = True

            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(
                        f"{id} - Retrying... (Attempt {retry_count + 1}/{max_retries})"
                    )
                    time.sleep(2.5**retry_count + random.uniform(4, 6))
                else:
                    raise Exception(f"Max retries ({max_retries}) reached for ID {id}.")

        if success and html:
            soup = BeautifulSoup(html, "html.parser")
            file_items = soup.find_all("a", class_="wpfd-file-link")
            return file_items
        else:
            return []

    finally:
        driver.quit()


def get_file_links(category_ids, max_workers=4):
    lst = []

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Processing...", total=len(category_ids))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(process_id, id, index, len(category_ids)): id
                for index, id in enumerate(category_ids)
                if id
            }

            for future in concurrent.futures.as_completed(future_to_id):
                id_ = future_to_id[future]
                try:
                    file_items = future.result()
                    lst.extend(file_items)
                except Exception as e:
                    raise e
                progress.update(task, advance=1)

    return list(set(lst))


def get_terms(smoke_test, smoke_test_n, max_workers, lm):
    term_parser = make_termparser()

    glossary_url = "https://meinsvwissen.de/glossar/"

    response = requests.get(glossary_url)
    soup = BeautifulSoup(response.content, "html.parser")

    elementors = soup.find_all(class_="elementor-toggle-item")
    if smoke_test:
        elementors = elementors[:smoke_test_n]
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

    n = len(elementors)
    results = [None] * n

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Parsing terms...", total=n)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(parse_term, elem): idx
                for idx, elem in enumerate(elementors)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()
                progress.update(task, advance=1)

    return pl.DataFrame(results)

def scrape_scc():
    """
    Scraping Student council committees
    """
    schema = SCCSchema.to_pydantic_model()
    url = "https://www.bildungsserver.de/schule/gremien-der-schuelervertretung-sm-12681-de.html"
    res = requests.get(url)
    res.raise_for_status()
    soup = BeautifulSoup(res.content, "html.parser")
    cards = soup.find_all("section", class_="a5-section-linklist")[1:]
    objs = []
    for card in cards:
        name = card.find("h4").text
        website = card.find("a", class_="a5-theme-linklist-item-headline-link")["href"]
        jurisdiction = [
            key for key, value in valid_jurisdictions.items() if value == name
        ][0]
        detail_link = card.find("a", title="Mehr Info")["href"]
        res = requests.get("https://www.bildungsserver.de" + detail_link)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")
        description = soup.find("div", class_="ym-gbox-left").find_all("p")[3].text
        obj = {
            "name": name,
            "website": website,
            "jurisdiction": jurisdiction,
            "description": description,
        }
        validated = schema.model_validate(obj).model_dump()
        objs.append(validated)

    # manually add Saarland (missing from website) but hoping for addition in the future
    if len(objs) == 16:
        with open("static_data/saarland_scc.json") as f:
            obj = json.loads(f.read())
        validated = schema.model_validate(obj).model_dump()
        objs.append(validated)

    assert len(objs) == 17

    return pl.from_dicts(objs)

def scrape_svtipps(sample_k=-1):
    schema = SVTippsSchema.to_pydantic_model()
    sanitizer = Sanitizer()
    base_url = "https://svtipps.de"
    
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    
    nav_tag = soup.find("nav").find("ul")
    if not nav_tag:
        raise Exception("No nav tag found on the main page")
    
    nav_links = nav_tag.find_all("a", href=True)
    
    url_hierarchy = {}
    current_category = None
    current_subcategory = None
    
    for link in nav_links:
        href = link["href"]
        text = link.get_text(strip=True)
        if text == "Downloads":
            continue
        data_level = link["data-level"]
        
        if href.startswith("/") and href != "/":
            full_url = base_url + href
        elif href.startswith(base_url) and href != base_url and href != base_url + "/":
            full_url = href
        else:
            raise RuntimeError("Invalid URL ", full_url)
        
        if data_level == "1":
            current_category = text
            current_subcategory = None
            url_hierarchy[full_url] = {
                "category": text,
                "subcategory": None
            }
        elif data_level == "2" and current_category:
            current_subcategory = text
            url_hierarchy[full_url] = {
                "category": current_category,
                "subcategory": None 
            }
        elif data_level == "3" and current_category and current_subcategory:
            url_hierarchy[full_url] = {
                "category": current_category,
                "subcategory": current_subcategory
            }
    
    urls_to_scrape = list(url_hierarchy.keys())
    
    if sample_k > 0:
        urls_to_scrape = list(urls_to_scrape)[:sample_k]
    
    objs = []
    
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Scraping SVTipps pages...", total=len(urls_to_scrape))
        
        for url in urls_to_scrape:
            time.sleep(random.uniform(0.5, 1.5))
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            page_soup = BeautifulSoup(response.content, "html.parser")
            
            content_div = page_soup.find("div", {"id": "content"})
            if not content_div:
                print(f"No content div found for {url}")
                progress.update(task, advance=1)
                raise RuntimeError("Invalid Page?!: ", url)
            
            title_tag = page_soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else url.split("/")[-2] or "Unknown"
            
            hierarchy_info = url_hierarchy.get(url, {"category": None, "subcategory": None})
            category = hierarchy_info["category"]
            subcategory = hierarchy_info["subcategory"]
            
            html_content = sanitizer.sanitize(str(content_div))
            
            obj = {
                "title": title,
                "url": url,
                "html_content": html_content,
                "category": category,
                "subcategory": subcategory,
            }
            
            validated = schema.model_validate(obj).model_dump()
            objs.append(validated)
        
            progress.update(task, advance=1)
    
    return pl.from_dicts(objs)