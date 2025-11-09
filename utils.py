# small helper utils (kept minimal)
import json

def safe_load_json(s):
    try:
        return json.loads(s)
    except:
        return None
