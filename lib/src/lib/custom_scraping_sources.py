from lib.tree_functions import find_node_by_id
import requests
from rich.progress import Progress, TimeRemainingColumn, BarColumn, TextColumn
import concurrent.futures
import polars as pl
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from lib.llm_parsers import make_termparser
import random
from lib.config import valid_jurisdictions
import dspy

def get_link_info(url, max_retries=3, retry_delay=3):
    jitter = random.uniform(2, 4)
    time.sleep(jitter)
    for attempt in range(max_retries):
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if content_type:
                    # stream can be odt
                    file_type = content_type.split("/")[-1]
                else:
                    file_type = "unknown"
                return True, file_type
            else:
                return False, "unknown"
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            else:
                return False, "unknown"


def process_link(link, root_node):
    category_id = int(link.get("data-category_id", 0))
    data_id = int(link.get("data-id", 0))
    title = link.get("title", "")
    category_title = find_node_by_id(root_node, str(category_id)).name
    download_link = f"https://meinsvwissen.de/download/{category_id}/so-weird-that/{data_id}/this-does-not-matter".lower()

    is_valid, file_type = get_link_info(download_link)

    if is_valid:
        return {
            "data_id": data_id,
            "data_category_id": category_id,
            "title": title,
            "category_title": category_title,
            "download_link": download_link,
            "file_type": file_type,
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
                    print(f"{id} - Retrying... (Attempt {retry_count + 1}/{max_retries})")
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
                for index, id in enumerate(category_ids) if id  
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