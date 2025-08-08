# ----------  app.py (CORRIGÉ)  ---------------------------------------
import uuid, json, pathlib, re, unicodedata
from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd, joblib, numpy as np
import config                     # ton fichier config.py (chemins, features…)
from datetime import datetime     # <-- ➊ IMPORT MANQUANT AJOUTÉ

# --------------------------------------------------------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = "change-me"

# --- ➋ FILTRE DE TEMPLATE MANQUANT AJOUTÉ ---
@app.template_filter('datetimeformat')
def format_datetime(value, format='%Y-%m-%d'):
    """
    Formate une chaîne de date ISO (ex: "2024-05-21T10:30:00.123") en une chaîne lisible.
    Utilisation dans le template : {{ ma_date | datetimeformat }}
    """
    if not value:
        return ""
    try:
        # Convertit la chaîne de caractères en un objet datetime
        dt_object = datetime.fromisoformat(value)
        # Formate l'objet datetime en une chaîne selon le format demandé
        return dt_object.strftime(format)
    except (TypeError, ValueError):
        # Si la conversion échoue, retourne la valeur originale
        return value
# -------------------------------------------------

# ------------ Modèle, colonnes, base joueurs ------------------------
model         = joblib.load(config.MODEL_PATH)
training_cols = joblib.load(config.COLUMNS_PATH)
player_db     = pd.read_pickle(config.PLAYER_DB_PATH)

# ------------ Listes d’auto-complétion ------------------------------
player_names = sorted(player_db['name'].unique())

tour_names = [
   "Australian Open", "Roland-Garros", "Wimbledon", "US Open",
   "Toronto", "Cincinnati", "Paris-Bercy", "Monte-Carlo",
   "Indian Wells", "Miami", "Madrid", "Rome"
]

# ------------ Persistance JSON --------------------------------------
STORE = pathlib.Path("predictions.json")

def load_predictions():
    if STORE.exists():
        return json.loads(STORE.read_text(encoding="utf-8"))
    return {}

def save_predictions(data: dict):
    STORE.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                     encoding="utf-8")

predictions = load_predictions()

# ------------ Helpers ------------------------------------------------
def normalize_name(n: str) -> str:
    n = ''.join(c for c in unicodedata.normalize('NFD', n)
                if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-zA-Z]', '', n).lower()

def get_player_stats(name: str) -> pd.Series:
    if 'norm' not in player_db.columns:
        player_db['norm'] = player_db['name'].apply(normalize_name)
    try:
        return player_db[player_db['norm'] == normalize_name(name)].iloc[0]
    except IndexError:
        d = {f: .5 for f in config.BASE_FEATURES if 'pct' in f or 'rate' in f}
        d.update({'rank': 150., 'age': 27., 'ht': 185., 'hand': 'R', 'form': .5})
        return pd.Series(d)

def default_val(f):
    return 0.5 if any(k in f for k in ('pct', 'rate', 'form')) else 150.

def predict(p1: str, p2: str, surface: str) -> float:
    p1s, p2s = get_player_stats(p1), get_player_stats(p2)
    row = {}
    for feat in config.BASE_FEATURES + ['form']:
        row[f'{feat}_diff'] = p1s.get(feat, default_val(feat)) - \
                              p2s.get(feat, default_val(feat))
    row.update({'p1_hand': p1s.get('hand', 'R'),
                'p2_hand': p2s.get('hand', 'R'),
                'surface': surface})
    X = pd.DataFrame([row])
    X = pd.get_dummies(X, columns=config.CATEGORICAL_FEATURES, dummy_na=True)
    X = X.reindex(columns=training_cols, fill_value=0)
    return model.predict_proba(X)[0, 1]

def compute_stats(preds: dict):
    decided  = [p for p in preds.values() if p["status"] != "pending"]
    n_good   = sum(p["status"] == "success" for p in decided)
    n_total  = len(decided)
    hit_rate = round(100 * n_good / n_total, 2) if n_total else None
    return {
        "decided": n_total,
        "good":    n_good,
        "bad":     n_total - n_good,
        "hit":     hit_rate
    }

# ------------ Routes -------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        players1_list    = request.form.getlist("player1")
        players2_list    = request.form.getlist("player2")
        surfaces_list    = request.form.getlist("surface")
        tournaments_list = request.form.getlist("tournament")

        for p1_form, p2_form, surf, tourn in zip(players1_list, players2_list, surfaces_list, tournaments_list):
            p1_form = p1_form.strip()
            p2_form = p2_form.strip()
            tourn = tourn.strip() or "?"

            if not p1_form or not p2_form:
                continue

            if p1_form.lower() < p2_form.lower():
                p1_model, p2_model = p1_form, p2_form
                inverted = False
            else:
                p1_model, p2_model = p2_form, p1_form
                inverted = True
            
            prob_model = predict(p1_model, p2_model, surf)
            final_prob = 1.0 - prob_model if inverted else prob_model
            prob = round(final_prob * 100, 2)
            uid = str(uuid.uuid4())
            
            predictions[uid] = {"p1": p1_form, "p2": p2_form, "surface": surf,
                                "tournament": tourn, "prob": prob,
                                "status": "pending"}

        save_predictions(predictions)
        return redirect(url_for("index"))

    return render_template("index.html",
                           preds=predictions,
                           players=player_names,
                           tours=tour_names)


@app.route("/history")
def history():
    stats = compute_stats(predictions)
    # Tri corrigé pour gérer les prédictions sans timestamp
    ordered = dict(sorted(predictions.items(),
                          key=lambda kv: kv[1].get("timestamp", ""),
                          reverse=True))
    return render_template("history.html",
                           preds=ordered,
                           stats=stats)

@app.post("/update/<uid>")
def update(uid):
    status = request.json.get("status")
    if uid in predictions and status in ("success", "fail"):
        predictions[uid]["status"] = status
        # Utilise l'import de datetime en haut du fichier
        predictions[uid]["timestamp"] = datetime.now().isoformat()
        save_predictions(predictions)
        return jsonify(ok=True)
    return jsonify(ok=False), 404

@app.post("/delete/<uid>")
def delete(uid):
    if uid in predictions:
        predictions.pop(uid)
        save_predictions(predictions)
        return jsonify(ok=True)
    return jsonify(ok=False), 404

# --------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)