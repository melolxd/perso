import pandas as pd
import joblib
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

import config

try:
    model = joblib.load(config.MODEL_PATH)
    training_columns = joblib.load(config.COLUMNS_PATH)
    player_db = pd.read_pickle(config.PLAYER_DB_PATH)
    print("Modèle, colonnes et base de données joueurs chargés avec succès.")
except FileNotFoundError as e:
    print(f"Erreur : Un fichier requis est manquant : {e}")
    print("Veuillez d'abord exécuter le script 'train.py' pour générer les fichiers nécessaires.")
    exit()

def get_player_stats(player_name):
    """Récupère les dernières statistiques d'un joueur."""
    try:
        player_series = player_db[player_db['name'].str.contains(player_name, case=False, na=False)].iloc[0]
        return player_series
    except IndexError:
        print(f"Avertissement : Joueur '{player_name}' non trouvé. Utilisation de stats par défaut.")
        # Ajout de toutes les nouvelles colonnes aux valeurs par défaut
        default_stats = {col: 0.5 for col in config.BASE_FEATURES if 'pct' in col or 'rate' in col}
        default_stats.update({'rank': 200, 'age': 27, 'ht': 185, 'hand': 'R', 'form': 0.5})
        return pd.Series(default_stats)

### MODIFICATION ICI : La fonction retourne maintenant un dictionnaire complet ###
def predict_match_details(player1_name, player2_name, surface, tournament="Tournoi"):
    """Prédit un match et retourne un dictionnaire détaillé pour le frontend."""
    p1_stats = get_player_stats(player1_name)
    p2_stats = get_player_stats(player2_name)
    
    # 1. Préparer les données pour la prédiction (les différences)
    match_data_for_model = {}
    
    # Toutes les caractéristiques à calculer pour la différence, y compris la 'form'
    all_features = config.BASE_FEATURES + ['form']

    for feature in all_features:
        # Gérer les valeurs manquantes avec des défauts logiques
        p1_val = p1_stats.get(feature, 0.5 if 'pct' in feature or 'rate' in feature or 'form' in feature else 200)
        p2_val = p2_stats.get(feature, 0.5 if 'pct' in feature or 'rate' in feature or 'form' in feature else 200)
        match_data_for_model[f'{feature}_diff'] = p1_val - p2_val
        
    match_data_for_model['p1_hand'] = p1_stats.get('hand', 'R')
    match_data_for_model['p2_hand'] = p2_stats.get('hand', 'R')
    match_data_for_model['surface'] = surface
    
    match_df = pd.DataFrame([match_data_for_model])
    match_df_encoded = pd.get_dummies(match_df, columns=config.CATEGORICAL_FEATURES, dummy_na=True)
    match_df_aligned = match_df_encoded.reindex(columns=training_columns, fill_value=0)
    
    # 2. Obtenir la probabilité
    p1_probability = model.predict_proba(match_df_aligned)[:, 1][0]
    
    # 3. Préparer le dictionnaire détaillé pour l'affichage
    details = {}
    for feature in all_features:
        details[feature] = {
            'p1': p1_stats.get(feature),
            'p2': p2_stats.get(feature),
            'diff': match_data_for_model.get(f'{feature}_diff', 0)
        }

    # 4. Formater le résultat final
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

# --- EXÉCUTION POUR GÉNÉRER LA SORTIE ---
if __name__ == "__main__":
    print("\n" + "="*60)
    print("      GÉNÉRATION DES DONNÉES DE PRÉDICTION")
    print("="*60)
    
    matches_to_predict = [
        {"p1": "Taylor Fritz", "p2": "Ben Shelton", "surface": "Hard", "tournament": "Tournoi de Toronto"},
        {"p1": "Alexander Zverev", "p2": "Karen Khachanov", "surface": "Hard", "tournament": "Tournoi de Toronto"}
    ]
    
    results_list = []
    for match in matches_to_predict:
        match_details = predict_match_details(
            player1_name=match["p1"],
            player2_name=match["p2"],
            surface=match["surface"],
            tournament=match["tournament"]
        )
        results_list.append(match_details)
        print(f"Prédiction calculée pour : {match['p1']} vs {match['p2']}")

    # Ici, dans une vraie application web (avec Flask par exemple), vous passeriez `results_list` à votre template.
    # Par exemple : return render_template('predictions.html', matches=results_list)
    
    print("\nDonnées prêtes à être envoyées au template HTML.")
    import json
    print(json.dumps(results_list[0], indent=2)) # Affiche un exemple du dictionnaire généré