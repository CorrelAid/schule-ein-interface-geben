import polars as pl
import requests
from typing import Any, Dict, List, Optional
from lib.models import PublicationSchema
import time
import random
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

schema = PublicationSchema.to_pydantic_model()


def get_zotero_api_data(
    api_url: str = "https://api.zotero.org/groups/6066861/items", sample_k=-1
) -> List[Dict[str, Any]]:
    params = {"format": "json", "include": "csljson", "limit": 100}
    resp = requests.get(api_url, params=params)
    resp.raise_for_status()
    jason = resp.json()
    if sample_k != -1:
        jason = random.choices(jason, k=sample_k)
    res = []
    for elem in jason:
        time.sleep(random.uniform(0.25, 0.5))
        main = elem["csljson"]
        # <userOrGroupPrefix>/items/<itemKey>/tags
        url = f"https://api.zotero.org/groups/6066861/items/{elem['key']}/tags"
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        jason = resp.json()
        tags = [item["tag"] for item in jason]
        res.append({"main": main, "tags": tags})

    return res


def parse_date_parts(date_parts: List[List[int]]) -> Optional[str]:
    if not date_parts or not date_parts[0]:
        return None
    parts = date_parts[0]
    if len(parts) == 1:
        return str(parts[0])
    return "-".join(str(p).zfill(2) for p in parts)


def download_pdf(
    url: str,
    logger,
    timeout=(30, 40),
    max_retries: int = 3,
    backoff_factor: float = 0.5,
) -> bytes:
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        response = session.get(
            url, stream=True, timeout=timeout, verify=certifi.where()
        )
        response.raise_for_status()

    except requests.exceptions.SSLError as ssl_err:
        logger.warning(
            f"SSL verification failed for {url}: {ssl_err}. Retrying without verification."
        )
        response = session.get(url, stream=True, timeout=timeout, verify=False)
        response.raise_for_status()

    pdf_chunks = []
    total_bytes = 0
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            pdf_chunks.append(chunk)
            total_bytes += len(chunk)
    return b"".join(pdf_chunks)


def convert_zotero_api_results(data, logger) -> pl.DataFrame:
    raw_records = []
    for obj in data:
        csl = obj["main"]
        tags = obj["tags"]
        rec = {
            "key": csl.get("id", ""),
            "type": csl.get("type", ""),
            "title": csl.get("title", ""),
            "abstract": csl.get("abstract") or None,
            "date": parse_date_parts(csl.get("issued", {}).get("date-parts", [])),
            "url": csl.get("URL", ""),
        }

        assert "https" in rec["url"], f"not a valid url: {rec['key']}, {rec['title']}"

        try:
            binary = download_pdf(rec["url"], logger)
        except requests.exceptions.ReadTimeout:
            logger.warning(f"Timeout for {rec['url']}")
            continue
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error for {rec['url']}: {e}")
            continue
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error for {rec['url']}: {e}")
            continue

        rec["pdf_binary"] = binary

        if isinstance(csl.get("author"), list):
            authors = [
                " ".join(filter(None, (a.get("given"), a.get("family"))))
                if a.get("given", "") != ""
                else a.get("family", "")
                for a in csl["author"]
            ]
            rec["authors"] = authors
        else:
            raise ValueError(
                f"No or invalid authors for {rec['key']}, {rec['title']}. Got: {csl.get('author')}"
            )
        if len(tags) > 0:
            for tag_type in ["jurisdiction", "school_type"]:
                temp = [t for t in tags if tag_type in t]
                if len(temp) > 1:
                    raise ValueError(f"More than one {tag_type}")
                if temp:
                    temp = temp[0].split(":")[1]
                    if tag_type == "jurisdiction":
                        temp = temp.upper()
                    rec[tag_type] = temp
            rec["tags"] = [
                t
                for t in tags
                if not t.startswith("jurisdiction") and not t.startswith("school_type")
            ]

        raw_records.append(rec)
    validated = [schema.model_validate(r).model_dump() for r in raw_records]

    df = pl.DataFrame(validated)
    return df
