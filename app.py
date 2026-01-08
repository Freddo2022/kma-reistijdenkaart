from flask import Flask, request, jsonify, send_from_directory, make_response, g
import csv
import os
import requests
import time
import sqlite3
from functools import wraps
from flask_compress import Compress
Compress(app)


app = Flask(__name__)

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

DTM_URL = "https://www.kilometerafstanden.nl/dtm-reistijdenkaart/dtm_pc4.csv"
DTM_FILE = "dtm_pc4.csv"
DTM_DB = "dtm_pc4.sqlite"

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
    - Mooie URL param: ?t=<token>
    - Backward compatible: ?key=<key>
    - Header blijft werken: Authorization: Bearer <key>
    """
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()

    token = (request.args.get("t") or "").strip()
    if token:
        return token

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
# DATA: DOWNLOAD CSV
# ---------------------------------------------------------

def download_dtm():
    if not os.path.exists(DTM_FILE):
        print("DTM CSV downloaden...")
        r = requests.get(DTM_URL, stream=True, timeout=60)
        r.raise_for_status()
        with open(DTM_FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

# ---------------------------------------------------------
# DATA: CSV -> SQLITE (1x)
# ---------------------------------------------------------

def build_sqlite_from_csv(csv_path: str, db_path: str):
    # Als DB al bestaat: klaar
    if os.path.exists(db_path) and os.path.getsize(db_path) > 0:
        print("SQLite bestaat al, import overslaan:", db_path)
        return

    print("SQLite bouwen vanuit CSV...")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dtm (
            pc4_from TEXT NOT NULL,
            pc4_to   TEXT NOT NULL,
            time_min INTEGER NOT NULL,
            distance_dm INTEGER NOT NULL,
            PRIMARY KEY (pc4_from, pc4_to)
        )
    """)

    # Sneller importeren (oké omdat de DB read-only gebruikt wordt)
    cur.execute("PRAGMA journal_mode = OFF;")
    cur.execute("PRAGMA synchronous = OFF;")
    cur.execute("PRAGMA temp_store = MEMORY;")

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        first_line = f.readline()
        delimiter = ";" if ";" in first_line else ","
        f.seek(0)
        reader = csv.DictReader(f, delimiter=delimiter)

        batch = []
        n = 0

        for row in reader:
            try:
                A = row["pc4_from"].strip().zfill(4)
                B = row["pc4_to"].strip().zfill(4)

                duration_s = float(row["duration_s"])
                distance_m = float(row["distance_m"])

                time_min = int(round(duration_s / 60.0))

                # compact: 0.1 km resolutie (100m) -> int
                distance_dm = int(round(distance_m / 100.0))

                # CSV heeft alleen A->B, maar jij wil ook B->A:
                batch.append((A, B, time_min, distance_dm))
                batch.append((B, A, time_min, distance_dm))

                n += 1
            except Exception:
                continue

            if len(batch) >= 20000:
                cur.executemany("INSERT OR REPLACE INTO dtm VALUES (?,?,?,?)", batch)
                conn.commit()
                batch.clear()
                print("... basis-rijen verwerkt:", n)

        if batch:
            cur.executemany("INSERT OR REPLACE INTO dtm VALUES (?,?,?,?)", batch)
            conn.commit()

    # Indexen (cruciaal voor performance)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_from ON dtm(pc4_from)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_from_time ON dtm(pc4_from, time_min)")
    conn.commit()

    # DB compacter maken
    cur.execute("VACUUM;")
    conn.close()

    print("SQLite klaar:", db_path)

# ---------------------------------------------------------
# SQLITE CONNECTION (per request)
# ---------------------------------------------------------

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DTM_DB)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()

# ---------------------------------------------------------
# INITIALISATIE
# ---------------------------------------------------------

download_dtm()
build_sqlite_from_csv(DTM_FILE, DTM_DB)
print("DTM SQLite ready")

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

    # Optioneel: kaart kan vaak met max_min werken (scheelt CPU/RAM/IO/JSON)
    max_min = request.args.get("max_min", type=int)

    db = get_db()

    if max_min is not None:
        rows = db.execute("""
            SELECT pc4_to AS dest, time_min, distance_dm
            FROM dtm
            WHERE pc4_from = ? AND time_min <= ?
            ORDER BY pc4_to
        """, (origin, max_min)).fetchall()
    else:
        rows = db.execute("""
            SELECT pc4_to AS dest, time_min, distance_dm
            FROM dtm
            WHERE pc4_from = ?
            ORDER BY pc4_to
        """, (origin,)).fetchall()

    # Compact terug: arrays i.p.v. list van dicts
    dest = []
    t = []
    d = []
    for r in rows:
        dest.append(r["dest"])
        t.append(int(r["time_min"]))
        d.append(int(r["distance_dm"]))  # 0.1 km

    return jsonify({"origin_pc4": origin, "count": len(dest), "dest": dest, "t": t, "d": d})

@app.route("/api/v1/route")
@require_api_key
def api_v1_route():
    origin = request.args.get("origin", "").strip().zfill(4)
    dest = request.args.get("dest", "").strip().zfill(4)

    if not origin or not dest:
        return jsonify({"error": "origin and dest parameters required"}), 400

    db = get_db()
    row = db.execute("""
        SELECT time_min, distance_dm
        FROM dtm
        WHERE pc4_from = ? AND pc4_to = ?
    """, (origin, dest)).fetchone()

    if not row:
        return jsonify({"error": "route not found"}), 404

    return jsonify({
        "origin_pc4": origin,
        "dest_pc4": dest,
        "time_min": int(row["time_min"]),
        "distance_km": round(int(row["distance_dm"]) / 10.0, 1)
    })

@app.route("/api/v1/origins")
@require_api_key
def api_v1_origins():
    db = get_db()
    rows = db.execute("SELECT DISTINCT pc4_from AS o FROM dtm ORDER BY o").fetchall()
    return jsonify({"origins": [r["o"] for r in rows]})

@app.route("/api/v1/nearest-location")
@require_api_key
def nearest_location():
    return jsonify({"error": "nearest-location not implemented"}), 501

# ---------------------------------------------------------
# LEGACY ENDPOINTS (blijven werken, maar nu uit SQLite)
# ---------------------------------------------------------

@app.route("/dtm")
def get_dtm():
    origin = request.args.get("origin")
    if not origin:
        return jsonify({"error": "origin parameter required"}), 400

    origin = origin.strip().zfill(4)

    max_min = request.args.get("max_min", type=int)
    db = get_db()

    if max_min is not None:
        rows = db.execute("""
            SELECT pc4_to AS dest, time_min, distance_dm
            FROM dtm
            WHERE pc4_from = ? AND time_min <= ?
            ORDER BY pc4_to
        """, (origin, max_min)).fetchall()
    else:
        rows = db.execute("""
            SELECT pc4_to AS dest, time_min, distance_dm
            FROM dtm
            WHERE pc4_from = ?
            ORDER BY pc4_to
        """, (origin,)).fetchall()

    # Legacy: list van dicts (maar wel uit DB)
    result = [
        {
            "dest_pc4": r["dest"],
            "time_min": int(r["time_min"]),
            "distance_km": int(round(int(r["distance_dm"]) / 10.0))
        }
        for r in rows
    ]

    return jsonify({"origin_pc4": origin, "count": len(result), "results": result})

@app.route("/origins")
def get_origins():
    db = get_db()
    rows = db.execute("SELECT DISTINCT pc4_from AS o FROM dtm ORDER BY o").fetchall()
    return jsonify({"origins": [r["o"] for r in rows]})

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
