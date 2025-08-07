# inspect_db.py
import pandas as pd
import config

# C'est une astuce pour que pandas nous montre plus de choses dans le terminal
pd.set_option('display.max_rows', 200)       # Affiche jusqu'à 200 lignes
pd.set_option('display.max_columns', 50)     # Affiche jusqu'à 50 colonnes
pd.set_option('display.width', 1000)         # Utilise plus de largeur dans le terminal

print(f"--- Chargement du fichier '{config.PLAYER_DB_PATH}' ---")
try:
    # On charge le fichier pkl exactement comme le feraient vos autres scripts
    player_db = pd.read_pickle(config.PLAYER_DB_PATH)
    print("✅ Fichier chargé avec succès !\n")

    # --- OPTION 1: Voir le début du tableau (les 5 premiers joueurs) ---
    print("--- APERÇU DES 5 PREMIERS JOUEURS ---")
    print(player_db.head())
    print("\n" + "="*80 + "\n")

    # --- OPTION 2: Voir les stats d'un joueur spécifique ---
    # Remplacez "Sinner" par le nom que vous voulez voir
    player_to_find = "Sinner"
    print(f"--- STATS DÉTAILLÉES POUR UN JOUEUR : {player_to_find.upper()} ---")
    player_row = player_db[player_db['name'].str.contains(player_to_find, case=False)]
    
    if not player_row.empty:
        # On affiche les statistiques sous une forme plus lisible (transposée)
        print(player_row.iloc[0].to_frame().T)
    else:
        print(f"Joueur '{player_to_find}' non trouvé.")
        
except FileNotFoundError:
    print("❌ Fichier introuvable. Avez-vous bien lancé le script 'train.py' ?")