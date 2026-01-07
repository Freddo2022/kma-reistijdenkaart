from flask import Flask, request, jsonify, send_from_directory, make_response
import csv
import os
import requests
import time
from functools import wraps

app = Flask(__name__)

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

DTM_URL = "https://www.kilometerafstanden.nl/dtm-reistijdenkaart/dtm_pc4.csv"
DTM_FILE = "dtm_pc4.csv"

dtm = {}  # { pc4_from: { pc4_to: {time_min, distance_km} } }

DEFAULT_ORIGIN_PC4 = "3521"   # (optioneel)

# ---------------------------------------------------------
# API AUTH + RATE LIMIT
# ---------------------------------------------------------

RAW_KEYS = os.environ.get("API_KEYS", "").strip()
VALID_KEYS = set([k.strip() for k in RAW_KEYS.split(",") if k.strip()])

RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "60"))
_rl_window = {}  # {key: (minute, count)}

def get_api_key():
    """
    Oplossing 1:
    - Mooie URL param: ?t=<token>
    - Backward compatible: ?key=<key>
    - Header blijft werken: Authorization: Bearer <key>
    """
    # 1) Header: Authorization: Bearer <key>
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()

    # 2) Query param: ?t=<token>  (nieuw)
    token = (request.args.get("t") or "").strip()
    if token:
        return token

    # 3) Query param: ?key=<key> (oude links blijven werken)
    return (request.args.get("key") or "").strip()

def rate_limit_ok(key: str) -> bool:
    if RATE_LIMIT_PER_MIN <= 0:
        return True
    now_min = int(time.time() // 60)
    win, cnt = _rl_window.get(key, (now_min, 0))
    if win != now_min:
        win, cnt = now_min, 0
    cnt += 1
    _rl_window[key] = (win, cnt)
    return cnt <= RATE_LIMIT_PER_MIN

def require_api_key(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not VALID_KEYS:
            return jsonify({"error": "API not configured (API_KEYS missing)"}), 503

        key = get_api_key()
        if not key:
            return jsonify({"error": "missing api key"}), 401
        if key not in VALID_KEYS:
            return jsonify({"error": "invalid api key"}), 403
        if not rate_limit_ok(key):
            return jsonify({"error": "rate limit exceeded"}), 429

        request.api_key = key
        return fn(*args, **kwargs)
    return wrapper

# ---------------------------------------------------------
# DATA LADEN
# ---------------------------------------------------------

def download_dtm():
    if not os.path.exists(DTM_FILE):
        print("DTM CSV downloaden...")
        r = requests.get(DTM_URL, stream=True)
        r.raise_for_status()
        with open(DTM_FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def load_dtm(path):
    global dtm
    dtm.clear()

    with open(path, newline="", encoding="utf-8-sig") as f:
        first_line = f.readline()
        delimiter = ";" if ";" in first_line else ","
        f.seek(0)

        reader = csv.DictReader(f, delimiter=delimiter)

        for row in reader:
            try:
                A = row["pc4_from"].strip().zfill(4)
                B = row["pc4_to"].strip().zfill(4)

                duration_s = float(row["duration_s"])
                distance_m = float(row["distance_m"])

                time_min = round(duration_s / 60.0, 2)
                distance_km = round(distance_m / 1000.0, 3)
            except Exception as e:
                print("Rij overgeslagen:", e)
                continue

            dtm.setdefault(A, {})
            dtm.setdefault(B, {})

            dtm[A][B] = {"time_min": time_min, "distance_km": distance_km}
            dtm[B][A] = {"time_min": time_min, "distance_km": distance_km}

# ---------------------------------------------------------
# INITIALISATIE
# ---------------------------------------------------------

download_dtm()
load_dtm(DTM_FILE)
print("Aantal PC4 origins geladen:", len(dtm))

# ---------------------------------------------------------
# CLIENT CONFIG
# ---------------------------------------------------------

CLIENTS = {
    "demo_290361": {
        "plan": "demo",
        "title": "DTM ReistijdenKaart | DriveTimeMatrix PC4-centroiden",
        "csv_max_rows": 20
    },
    "medialane_070126": {
        "plan": "pro",
        "title": "DTM ReistijdenKaart",
        "csv_max_rows": None
    }
}

# ---------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------

@app.route("/api/v1/me")
@require_api_key
def me():
    key = request.api_key
    cfg = CLIENTS.get(key, {"plan": "pro", "title": "DTM ReistijdenKaart"})
    return jsonify(cfg)

@app.route("/api/v1/dtm")
@require_api_key
def api_v1_dtm():
    origin = request.args.get("origin", "").strip()
    if not origin:
        return jsonify({"error": "origin parameter required"}), 400

    origin = origin.zfill(4)
    if origin not in dtm:
        return jsonify({"error": f"origin {origin} not found"}), 404

    result = [
        {
            "dest_pc4": dest,
            "time_min": int(round(values["time_min"])),
            "distance_km": round(values["distance_km"], 1)
        }
        for dest, values in dtm[origin].items()
    ]

    return jsonify({"origin_pc4": origin, "count": len(result), "results": result})

@app.route("/api/v1/route")
@require_api_key
def api_v1_route():
    origin = request.args.get("origin", "").strip().zfill(4)
    dest = request.args.get("dest", "").strip().zfill(4)

    if not origin or not dest:
        return jsonify({"error": "origin and dest parameters required"}), 400

    if origin not in dtm or dest not in dtm[origin]:
        return jsonify({"error": "route not found"}), 404

    values = dtm[origin][dest]
    return jsonify({
        "origin_pc4": origin,
        "dest_pc4": dest,
        "time_min": int(round(values["time_min"])),
        "distance_km": round(values["distance_km"], 1)
    })

@app.route("/api/v1/origins")
@require_api_key
def api_v1_origins():
    return jsonify({"origins": sorted(dtm.keys())})

@app.route("/api/v1/nearest-location")
@require_api_key
def nearest_location():
    return jsonify({"error": "nearest-location not implemented"}), 501

# ---------------------------------------------------------
# LEGACY ENDPOINTS (optioneel laten staan)
# ---------------------------------------------------------

@app.route("/dtm")
def get_dtm():
    origin = request.args.get("origin")
    if not origin:
        return jsonify({"error": "origin parameter required"}), 400

    origin = origin.strip().zfill(4)
    if origin not in dtm:
        return jsonify({"error": f"origin {origin} not found"}), 404

    result = [
        {
            "dest_pc4": dest,
            "time_min": int(round(values["time_min"])),
            "distance_km": int(round(values["distance_km"]))
        }
        for dest, values in dtm[origin].items()
    ]

    return jsonify({"origin_pc4": origin, "count": len(result), "results": result})

@app.route("/origins")
def get_origins():
    return jsonify({"origins": sorted(dtm.keys())})

# ---------------------------------------------------------
# UI: kaart alleen met token (HTML i.p.v. JSON bij fout)
# ---------------------------------------------------------

@app.route("/")
def home():
    token = get_api_key()

    if not token or token not in VALID_KEYS:
        html = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<title>Toegang vereist</title></head><body style='font-family:Arial;padding:40px'>"
            "<h2>🔒 Toegang vereist</h2>"
            "<p>Deze kaart is alleen toegankelijk met een geldige toegangstoken.</p>"
            "<p>Gebruik: <code>/?t=JOUW_TOKEN</code></p>"
            "</body></html>"
        )
        return make_response(html, 403)

    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    return send_from_directory(static_path, "index.html")

@app.route("/static/<path:filename>")
def static_files(filename):
    token = get_api_key()
    if not token or token not in VALID_KEYS:
        return make_response("Access denied", 403)

    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    return send_from_directory(static_path, filename)

@app.route("/pc4")
def pc4_geo():
    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    return send_from_directory(static_path, "pc4_gebieden.geojson")

@app.route("/health")
def health():
    return jsonify({"status": "ok"})
