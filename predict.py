# predict.py (v2.0 - Adapté pour STATS PAR SURFACE)
import pandas as pd
import joblib
import warnings
import json
import numpy as np
import config

warnings.filterwarnings("ignore", category=UserWarning)

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        elif isinstance(obj, np.floating): return float(obj)
        elif isinstance(obj, np.ndarray): return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

try:
    model = joblib.load(config.MODEL_PATH)
    training_columns = joblib.load(config.COLUMNS_PATH)
    player_db = pd.read_pickle(config.PLAYER_DB_PATH)
    print("Modèle, colonnes et base de données (par surface) chargés avec succès.")
except FileNotFoundError as e:
    print(f"Erreur : Fichier manquant : {e}. Lancez 'train.py' pour le générer.")
    exit()

def get_player_stats(player_name, surface):
    """Récupère les stats d'un joueur pour une surface, avec une solution de secours."""
    try:
        # Recherche la combinaison exacte nom + surface
        player_series = player_db.loc[(player_db['name'] == player_name) & (player_db.index.get_level_values('surface') == surface)]
        if player_series.empty: raise KeyError
        return player_series.iloc[0]
    except KeyError:
        # Solution de secours : le joueur n'a pas de stats pour cette surface.
        # On cherche sa fiche la plus récente, n'importe quelle surface.
        try:
            player_series = player_db[player_db['name'] == player_name].sort_values('tourney_date', ascending=False).iloc[0]
            print(f"Avertissement : '{player_name}' n'a pas de stats pour '{surface}'. Utilisation de ses stats générales les plus récentes.")
            return player_series
        except IndexError:
            # Si le joueur est totalement inconnu
            print(f"Avertissement : Joueur '{player_name}' totalement inconnu. Utilisation de stats par défaut.")
            default_stats = {f'rolling_{col}': 0.5 for col in config.BASE_FEATURES}
            default_stats.update({'rank': 200, 'age': 27, 'ht': 185, 'hand': 'R', 'form': 0.5, 'name': player_name})
            return pd.Series(default_stats)

def predict_match_details(player1_name, player2_name, surface, tournament="Tournoi"):
    p1_stats = get_player_stats(player1_name, surface)
    p2_stats = get_player_stats(player2_name, surface)

    match_data_for_model = {}
    
    # La liste des features 'rolling'
    features_to_diff = ['rank', 'age', 'ht', 'form'] + [f"rolling_{s}" for s in config.BASE_FEATURES if s not in ['ht','rank','age']]

    for feature in features_to_diff:
        p1_val = p1_stats.get(feature, 0.5)
        p2_val = p2_stats.get(feature, 0.5)
        diff_name = feature.replace('rolling_', '') + '_diff'
        match_data_for_model[diff_name] = p1_val - p2_val

    match_data_for_model['p1_hand'] = p1_stats.get('hand', 'R')
    match_data_for_model['p2_hand'] = p2_stats.get('hand', 'R')
    match_data_for_model['surface'] = surface

    match_df = pd.DataFrame([match_data_for_model])
    match_df_encoded = pd.get_dummies(match_df, columns=config.CATEGORICAL_FEATURES, dummy_na=True)
    match_df_aligned = match_df_encoded.reindex(columns=training_columns, fill_value=0)

    p1_probability = model.predict_proba(match_df_aligned)[:, 1][0]
    
    # Formater un résultat détaillé
    details = {}
    display_features = ['rank', 'age', 'ht', 'form'] + [f'rolling_{s}' for s in config.BASE_FEATURES if s not in ['ht', 'rank', 'age']]
    for feature in display_features:
        details[feature.replace("rolling_", "")] = {
            'p1': p1_stats.get(feature, 'N/A'),
            'p2': p2_stats.get(feature, 'N/A'),
            'diff': match_data_for_model.get(f"{feature.replace('rolling_', '')}_diff", 0)
        }

    result = {
        'tournament': tournament,
        'surface': surface,
        'player1': player1_name,
        'player2': player2_name,
        'p1_proba': p1_probability,
        'p2_proba': 1 - p1_probability,
        'details': details
    }
    return result

if __name__ == "__main__":
    print("\n" + "="*60)
    print("      GÉNÉRATION DES PRÉDICTIONS (PAR SURFACE)")
    print("="*60)

    matches_to_predict = [
        {"p1": "Taylor Fritz", "p2": "Ben Shelton", "surface": "Hard", "tournament": "Masters de Toronto"},
        {"p1": "Jannik Sinner", "p2": "Carlos Alcaraz", "surface": "Clay", "tournament": "Roland-Garros (Exemple Terre Battue)"},
        {"p1": "Jannik Sinner", "p2": "Carlos Alcaraz", "surface": "Grass", "tournament": "Wimbledon (Exemple Gazon)"}
    ]

    results_list = []
    for match in matches_to_predict:
        print(f"\n--- Prédiction pour : {match['p1']} vs {match['p2']} sur {match['surface']} ---")
        match_details = predict_match_details(match["p1"], match["p2"], match["surface"], match["tournament"])
        results_list.append(match_details)
        print(f"Probabilité {match['p1']}: {match_details['p1_proba']:.2%}")

    print("\n--- Données JSON pour le premier match ---")
    print(json.dumps(results_list[0], indent=2, cls=NumpyEncoder))