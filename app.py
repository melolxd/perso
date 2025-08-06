# app.py

import pandas as pd
import joblib
import warnings
from flask import Flask, render_template
import requests
from bs4 import BeautifulSoup
import unicodedata

import config

warnings.filterwarnings("ignore", category=UserWarning)

app = Flask(__name__)

# --- CHARGEMENT DES ARTEFACTS (ne change pas) ---
try:
    model = joblib.load(config.MODEL_PATH)
    training_columns = joblib.load(config.COLUMNS_PATH)
    player_db = pd.read_pickle(config.PLAYER_DB_PATH)
    print("Modèle, colonnes et base de données joueurs chargés.")
except FileNotFoundError:
    print("ERREUR : Fichiers du modèle non trouvés. Exécutez train.py d'abord.")
    exit()

# --- FONCTIONS UTILES (ne changent pas) ---
def normalize_name(name):
    return ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn').lower()

def get_player_stats(player_name):
    normalized_name = normalize_name(player_name)
    try:
        if 'normalized_name' not in player_db.columns:
            player_db['normalized_name'] = player_db['name'].apply(normalize_name)
        player_series = player_db[player_db['normalized_name'] == normalized_name].iloc[0]
        return player_series
    except (IndexError, KeyError):
        print(f"Avertissement : Joueur '{player_name}' non trouvé. Utilisation de stats par défaut.")
        default_stats = {'name': player_name, 'rank': 500, 'age': 27, 'ht': 185, 'hand': 'R', 'form': 0.5, 'ace_rate': 0.4, 'df_rate': 0.2, 'first_serve_in_pct': 0.6, 'first_serve_win_pct': 0.7, 'second_serve_win_pct': 0.5, 'bp_saved_pct': 0.6, 'service_games_win_pct': 0.8}
        return pd.Series(default_stats)

def predict_match_details(player1_name, player2_name, surface):
    p1_stats = get_player_stats(player1_name)
    p2_stats = get_player_stats(player2_name)
    match_data = {}
    details = {}

    features_to_calc = config.BASE_FEATURES + ['form']
    for feature in features_to_calc:
        p1_key = f'rolling_{feature}' if feature not in ['rank', 'age', 'ht', 'return_points_win_pct'] else feature
        p2_key = f'rolling_{feature}' if feature not in ['rank', 'age', 'ht', 'return_points_win_pct'] else feature
        
        # Gestion des valeurs par défaut pour les joueurs non trouvés
        default_val = 0.5 if 'pct' in feature or 'rate' in feature or 'form' in feature else 200
        p1_val = p1_stats.get(p1_key, default_val)
        p2_val = p2_stats.get(p2_key, default_val)

        diff = p1_val - p2_val
        match_data[f'{feature}_diff'] = diff
        details[feature] = {'p1': p1_val, 'p2': p2_val, 'diff': diff}
        
    match_data['p1_hand'] = p1_stats.get('hand', 'R')
    match_data['p2_hand'] = p2_stats.get('hand', 'R')
    match_data['surface'] = surface
    
    match_df = pd.DataFrame([match_data])
    match_df_encoded = pd.get_dummies(match_df, columns=config.CATEGORICAL_FEATURES, dummy_na=True)
    match_df_aligned = match_df_encoded.reindex(columns=training_columns, fill_value=0)
    
    probability = model.predict_proba(match_df_aligned)[:, 1][0]
    
    return {
        'player1': player1_name,
        'player2': player2_name,
        'surface': surface,
        'p1_proba': probability,
        'p2_proba': 1 - probability,
        'details': details
    }

# --- DÉFINITION DE LA PAGE WEB PRINCIPALE (MODIFIÉE) ---
@app.route('/')
def home():
    """Page d'accueil qui affiche les prédictions d'une liste de matchs manuelle."""
    
    ### MODIFICATION ICI ###
    # Au lieu de scraper, on définit nous-mêmes la liste des matchs à afficher.
    # Vous pouvez changer/ajouter/supprimer des matchs dans cette liste !
    
    matches_to_predict = [
        {'p1': 'Taylor Fritz', 'p2': 'Ben Shelton', 'surface': 'Hard', 'tournament': 'Tournoi de Toronto'},
        {'p1': 'Alexander Zverev', 'p2': 'Karen Khachanov', 'surface': 'Hard', 'tournament': 'Tournoi de Toronto'},
    ]
    
    print(f"Prédiction pour {len(matches_to_predict)} matchs définis manuellement.")
    
    predictions = []
    for match in matches_to_predict:
        try:
            prediction_details = predict_match_details(match['p1'], match['p2'], match['surface'])
            prediction_details['tournament'] = match['tournament']
            predictions.append(prediction_details)
        except Exception as e:
            print(f"Erreur lors de la prédiction pour {match['p1']} vs {match['p2']}: {e}")

    # Envoyer les prédictions à la page HTML
    return render_template('index.html', matches=predictions)


# --- DÉMARRAGE DE L'APPLICATION (ne change pas) ---
if __name__ == "__main__":
    app.run(debug=True)