from flask import Flask, request, jsonify, send_from_directory
import csv
import os

DTM_FILE = "dtm_pc4.csv"   # jouw bestand

# datastructuur { pc4_from: { pc4_to: {time_min, distance_km} } }
dtm = {}


def load_dtm(path):
    global dtm
    dtm.clear()

    with open(path, newline='', encoding='utf-8-sig') as f:
        # delimiter automatisch detecteren: ; of ,
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
                print("Rij overgeslagen:", row, e)
                continue

            # -------------------------
            # A → B opslaan
            # -------------------------
            if A not in dtm:
                dtm[A] = {}
            dtm[A][B] = {
                "time_min": time_min,
                "distance_km": distance_km
            }

            # -------------------------
            # B → A automatisch toevoegen (symmetrisch maken)
            # -------------------------
            if B not in dtm:
                dtm[B] = {}
            dtm[B][A] = {
                "time_min": time_min,
                "distance_km": distance_km
            }


# laad de data bij start
load_dtm(DTM_FILE)
print("Aantal origins geladen:", len(dtm.keys()))

app = Flask(__name__)


# ---------------------------------------------------------
#  CORRECT EN COMPLEET: get_dtm() functie
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
            "time_min": values["time_min"],
            "distance_km": values["distance_km"]
        }
        for dest, values in dtm[origin].items()
    ]

    return jsonify({
        "origin_pc4": origin,
        "count": len(result),
        "results": result
    })


# ---------------------------------------------------------
#  FRONTEND & GEOFILES
# ---------------------------------------------------------

@app.route("/")
def home():
    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    return send_from_directory(static_path, "index.html")


@app.route("/origins")
def get_origins():
    origins = sorted(list(dtm.keys()))
    return jsonify({"origins": origins})


@app.route("/pc4")
def pc4_geo():
    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    return send_from_directory(static_path, "pc4_gebieden.geojson")


# ---------------------------------------------------------
#  SERVER STARTEN
# ---------------------------------------------------------
if __name__ == "__main__":
    print("KmA DTM backend draait op http://localhost:8000")
    app.run(host="0.0.0.0", port=8000)

