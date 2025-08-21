from lib.legal_res_helpers import get_legal_resources
import json

def test_get_legal_resources():
    path = "static_data/legal_resources.json"
    with open(path) as f:
        cfg = json.load(f)

    get_legal_resources(cfg,True)