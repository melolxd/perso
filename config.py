# --- PARAMÈTRES GÉNÉRAUX ---
START_YEAR = 1968
END_YEAR = 2025
DATA_PATH = '.'

# --- PARAMÈTRES D'ENTRAÎNEMENT ---
### MODIFICATION ICI : Ajout de 'break_points_converted_pct' ###
BASE_FEATURES = [
    'rank', 'age', 'ht',
    'ace_rate', 'df_rate', 'first_serve_in_pct', 'first_serve_win_pct',
    'second_serve_win_pct', 'bp_saved_pct', 'service_games_win_pct',
    'return_points_win_pct', 'break_points_converted_pct' # Nouvelle feature !
]

CATEGORICAL_FEATURES = ['p1_hand', 'p2_hand', 'surface']
ROLLING_WINDOW = 10
MIN_PERIODS = 3

# --- NOMS DES FICHIERS SAUVEGARDÉS ---
MODEL_PATH = 'atp_predictor_model.joblib'
COLUMNS_PATH = 'atp_predictor_columns.joblib'
PLAYER_DB_PATH = 'atp_player_db.pkl'