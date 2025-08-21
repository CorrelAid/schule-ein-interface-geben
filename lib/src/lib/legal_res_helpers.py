import requests
import polars as pl
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from html_sanitizer import Sanitizer
import time
from lib.models import LegalResourceSchema
from rich.progress import Progress, TimeRemainingColumn, BarColumn, TextColumn

from html_sanitizer.sanitizer import DEFAULT_SETTINGS

DEFAULT_SETTINGS["attributes"] = {"a": ("name", "target", "title", "id", "rel")}


def create_chrome_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)


def scrape_legal_page(url, timeout=35):
    driver = create_chrome_driver()

    driver.get(url)

    time.sleep(5)

    if "wolterskluwer" in url:
        version = "wolterskluwer"
    else:
        version = "standard"

    if version == "standard":
        button_xpath = (
            "//span[contains(normalize-space(), 'Aktueller Gesamttext') "
            "or contains(normalize-space(), 'Aktuelle Gesamtausgabe')]"
        )

        button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        )

        button.click()
        time.sleep(5)

        content_class = "docbody"

        page_source = driver.page_source

        soup = BeautifulSoup(page_source, "html.parser")

        doc_body = soup.find(class_=content_class)
    elif version == "wolterskluwer":
        button_xpath = "//a[contains(text(), 'Gesamte Quelle anzeigen')]"
        button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        )

        button.click()
        time.sleep(5)

        ###### Scrolling through the page to load lazyload elements ಠ_ಠ
        scroll_increment_ratio = 25
        pause_time = 0.1

        last_position = driver.execute_script("return window.pageYOffset")
        total_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script(
                "window.scrollBy(0, arguments[0]);",
                total_height // scroll_increment_ratio,
            )
            time.sleep(pause_time)

            current_position = driver.execute_script("return window.pageYOffset")

            total_height = driver.execute_script("return document.body.scrollHeight")

            viewport_height = driver.execute_script("return window.innerHeight")

            if current_position + viewport_height >= total_height:
                break

            if current_position == last_position:
                break

            last_position = current_position
        ######

        content_class = "block-system-main-block"

        page_source = driver.page_source

        soup = BeautifulSoup(page_source, "html.parser")

        doc_body = soup.find(class_=content_class)

    driver.quit()
    if doc_body:
        return doc_body.prettify()
    else:
        raise RuntimeError("No element with class 'docbody' found.")


def get_legal_soup(url, class_=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    res = requests.get(url, headers=headers)
    res.raise_for_status()
    soup = BeautifulSoup(res.content, "html.parser")
    if class_ is not None:
        content = soup.find(class_=class_)
    else:
        content = soup.find("body")

    if content:
        return str(content)
    else:
        raise RuntimeError("Did not find law content")


def wait_jportal_load(url):
    driver = create_chrome_driver()

    driver.get(url)
    time.sleep(5)
    page_source = driver.page_source
    driver.quit()

    soup = BeautifulSoup(page_source, "html.parser")

    doc_body = soup.find(class_="docbody")
    if doc_body:
        return doc_body.prettify()
    else:
        raise RuntimeError("No element with class 'docbody' found.")

def get_legal_resources(cfg, debug=False, logger=None):
    """
    Very open data
    """
    sanitizer = Sanitizer(settings=DEFAULT_SETTINGS)
    schema = LegalResourceSchema.to_pydantic_model()
    law_resources = []
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        total = sum(len(entry["resources"]) for entry in cfg)
        task = progress.add_task("Getting legal resources...", total=total)
        for obj in cfg:
            jurisdiction = obj["jurisdiction"]
            for resource in obj["resources"]:
                if debug:
                    logger.info(resource)
                if resource["strategy"] == "selenium":
                    html = scrape_legal_page(resource["permalink"])
                elif resource["strategy"] == "wait_jportal_load":
                    html = wait_jportal_load(resource["permalink"])
                elif resource["strategy"] == "soup":
                    html = get_legal_soup(resource["permalink"], resource["area_class"])
                elif resource["strategy"] == "direct":
                    html = get_legal_soup(resource["permalink"])
                else:
                    raise ValueError(f"Not a valid strategy: {resource['strategy']}")
                sanitized = sanitizer.sanitize(html)
                validated = schema.model_validate(
                    {
                        "html": sanitized,
                        "title": resource["title"],
                        "type": resource["type"],
                        "jurisdiction": jurisdiction,
                        "url": resource.get("permalink", None)
                        if resource.get("permalink", None) is not None
                        else resource.get("url"),
                    }
                ).model_dump()
                law_resources.append(validated)
                progress.update(task, advance=1)

    df = pl.DataFrame(law_resources)
    return df
