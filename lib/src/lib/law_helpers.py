# types of law sources

# permalink and retrieval strat selenium_jportal
# permalink and soup: has to have area_class
# permalink and html_url
# url: perma not guaranteed

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import os


def scrape_law_page(url, version="standard", timeout=10):
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)

    time.sleep(4)
    if version == "standard":
        button_xpath = (
            "//span[contains(normalize-space(), 'Aktueller Gesamttext') "
            "or contains(normalize-space(), 'Aktuelle Gesamtausgabe')]"
        )

        button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        )

        button.click()
        time.sleep(3)
    
        content_class = "docbody"

        page_source = driver.page_source

        driver.quit()

        soup = BeautifulSoup(page_source, "html.parser")

        doc_body = soup.find(class_=content_class)
    elif version == "wolterskluwer":
        button_xpath = "//a[contains(text(), 'Gesamte Quelle anzeigen')]"
        button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        )

        button.click()
        time.sleep(3)
        scroll_increment_ratio = 20
        pause_time = 0.1

        last_position = driver.execute_script("return window.pageYOffset")
        total_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollBy(0, arguments[0]);", total_height // scroll_increment_ratio)
            time.sleep(pause_time)

            current_position = driver.execute_script("return window.pageYOffset")

            total_height = driver.execute_script("return document.body.scrollHeight")

            viewport_height = driver.execute_script("return window.innerHeight")

            if current_position + viewport_height >= total_height:
                break

            if current_position == last_position:
                break

            last_position = current_position

        content_class = "block-system-main-block"

        page_source = driver.page_source

        soup = BeautifulSoup(page_source, "html.parser")

        doc_body = soup.find(class_=content_class)

    driver.quit()
    if doc_body:
        return doc_body.prettify()
    else:
        print("No element with class 'docbody' found.")
        return None

def get_law_soup(url, class_):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    res = requests.get(url, headers=headers)
    res.raise_for_status()
    soup = BeautifulSoup(res.content)

    content = soup.find(class_=class_)

    if content:
        return str(content)
    else:
        raise RuntimeError("Did not find law content")


def wait_jportal_load(url):
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)
    time.sleep(4)
    page_source = driver.page_source
    driver.quit()
    
    soup = BeautifulSoup(page_source)

    doc_body = soup.find(class_="docbody")

    driver.quit()
    if doc_body:
        return doc_body.prettify()
    else:
        print("No element with class 'docbody' found.")
        return None
    