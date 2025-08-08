# config.py

# --- Chemins (inchangés) ---
DATA_PATH = "."
MODEL_PATH = "model/lgbm_model.joblib"
COLUMNS_PATH = "model/training_columns.joblib"
PLAYER_DB_PATH = "model/player_db.pkl"

# --- Paramètres du modèle et des features ---
START_YEAR = 2010
END_YEAR = 2025
ROLLING_WINDOW = 20  # Fenêtre pour les moyennes mobiles (en nombre de matchs)
MIN_PERIODS = 5      # Nombre de matchs minimum pour calculer une stat

# --- Listes de Features ---
# Caractéristiques de base extraites des données brutes
RAW_STATS = [
    'ace', 'df', 'svpt', '1stIn', '1stWon', '2ndWon', 'SvGms', 'bpSaved', 'bpFaced',
    'return_pts_won', 'return_pts_total'
]

# Caractéristiques calculées (pourcentages, ratios)
# NOTE: 'break_points_converted_pct' a été retiré car redondant avec 'service_games_win_pct' de l'adversaire
DERIVED_STATS = [
    'ace_rate', 'df_rate', 'first_serve_in_pct', 'first_serve_win_pct',
    'second_serve_win_pct', 'bp_saved_pct', 'service_games_win_pct',
    'return_points_win_pct'
]

# Caractéristiques fondamentales du joueur
PLAYER_INFO = ['rank', 'age', 'ht']

# NOUVEAU: Features composites
COMPOSITE_FEATURES = ['elo', 'h2h_win_pct']

# Toutes les features de base utilisées pour créer les différences
# 'form' est le nom de la moyenne mobile de la variable 'won'
BASE_FEATURES = PLAYER_INFO + DERIVED_STATS

# Features catégorielles à encoder
CATEGORICAL_FEATURES = ['p1_hand', 'p2_hand', 'surface']

ELO_K_FACTOR = 32
ELO_DEFAULT = 1500