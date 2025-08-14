import polars as pl
import requests
from typing import Any, Dict, List, Optional
from lib.models import PublicationSchema

# Build your Pydantic model class once
schema = PublicationSchema.to_pydantic_model()

def get_zotero_api_data(
    api_url: str = "https://api.zotero.org/groups/6066861/items"
) -> List[Dict[str, Any]]:
    params = {"format": "json", "include": "csljson", "limit": 100}
    resp = requests.get(api_url, params=params)
    resp.raise_for_status()
    return [elem["csljson"] for elem in resp.json()]

def parse_date_parts(date_parts: List[List[int]]) -> Optional[str]:
    if not date_parts or not date_parts[0]:
        return None
    parts = date_parts[0]
    if len(parts) == 1:
        return str(parts[0])
    return "-".join(str(p).zfill(2) for p in parts)

def convert_zotero_api_results(
    csl_items: List[Dict[str, Any]]
) -> pl.DataFrame:
    raw_records = []
    for csl in csl_items:
        rec = {
            "key":      csl.get("id", ""),
            "type":     csl.get("type", ""),
            "title":    csl.get("title", ""),
            "abstract": csl.get("abstract") or None,
            "date":     parse_date_parts(csl.get("issued", {}).get("date-parts", [])),
            "url":      csl.get("URL", "") or None,
        }

        if isinstance(csl.get("author"), list):
            authors = [
                " ".join(filter(None, (a.get("given",""), a.get("family",""))))
                for a in csl["author"]
            ]
            rec["authors"] = authors or None
        else:
            rec["authors"] = None

        kw = csl.get("keyword")
        if isinstance(kw, str):
            rec["tags"] = [k.strip() for k in kw.split(";") if k.strip()] or None
        elif isinstance(kw, list):
            rec["tags"] = [str(k).strip() for k in kw if str(k).strip()] or None
        else:
            rec["tags"] = None

        raw_records.append(rec)
    validated = [
        schema.model_validate(r).model_dump() 
        for r in raw_records
    ]

    df = pl.DataFrame(validated)
    return df

