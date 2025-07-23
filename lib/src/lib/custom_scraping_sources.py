
from lib.tree_functions import find_node_by_id
import requests
from rich.progress import Progress, TimeRemainingColumn, BarColumn, TextColumn
import concurrent.futures
import polars as pl
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import polars as pl
import random
import mimetypes
import markdown
from lib.config import pipeline_name, db_name, tree_json_path, download_subpage


def get_link_info(url):
    jitter = random.uniform(0.1, 0.5)
    time.sleep(jitter)
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if content_type:
                # stream can be odt
                file_type = content_type.split('/')[-1]
            else:
                file_type = 'unknown'
            return True, file_type
        else:
            return False, 'unknown'
    except requests.RequestException:
        return False, 'unknown'

def process_link(link, root_node):
    category_id = int(link.get('data-category_id', 0))
    data_id = int(link.get('data-id', 0))
    title = link.get('title', '')
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
            "file_type": file_type
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
                try:
                    result = future.result()
                    data.append(result)
                except Exception as e:
                    progress.print(f"[red]{str(e)}[/red]")
                progress.update(task, advance=1)

    df = pl.DataFrame(data)
    return df

def get_download_soup(wp_user, wp_pw, max_retries=3):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(5)  # Set implicit wait once

    retry_count = 0
    try:
        while retry_count < max_retries:
            try:
                driver.get("https://meinsvwissen.de/wp-admin/admin.php?page=wpfd")

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
                print(f"Error: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying... (Attempt {retry_count + 1}/{max_retries})")
                    # eponential backoff with jitter
                    time.sleep(2 ** retry_count + random.uniform(0, 1))
                else:
                    raise Exception(f"Max retries ({max_retries}) reached in get_download_soup.")
    finally:
        driver.quit()

def process_id(id, index, total, max_retries=3):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(5)

    retry_count = 0
    success = False
    html = None

    try:
        while retry_count < max_retries and not success:
            try:
                jitter = random.uniform(0.5, 2.0)
                driver.get(f"https://meinsvwissen.de/sv-archiv/#36-{id}")
                time.sleep(4 + jitter)

                container = driver.find_element(By.ID, "wpfd-elementor-category")
                html = container.get_attribute("innerHTML")
                success = True

            except Exception as e:
                print(f"Error: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying... (Attempt {retry_count + 1}/{max_retries})")
                    time.sleep(2 ** retry_count + random.uniform(0, 1))  
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
            }

            for future in concurrent.futures.as_completed(future_to_id):
                id = future_to_id[future]
                try:
                    file_items = future.result()
                    lst.extend(file_items)
                except Exception as e:
                    progress.print(f"[red]Error processing ID {id}: {e}[/red]")
                progress.update(task, advance=1)

    return list(set(lst))


