import pandas as pd
import glob
import numpy as np
import os
import joblib
from lightgbm import LGBMClassifier

# Importer la configuration
import config

# --- ÉTAPE 1 : CHARGEMENT (Inchangé) ---
def load_and_combine_matches(path, start_year, end_year):
    all_files = []
    print(f"Recherche des fichiers de {start_year} à {end_year} dans le dossier : {os.path.abspath(path)}")
    for year in range(start_year, end_year + 1):
        all_files.extend(glob.glob(os.path.join(path, f'atp_matches_{year}.csv')))
        all_files.extend(glob.glob(os.path.join(path, f'atp_matches_qual_chall_{year}.csv')))

    if not all_files:
        raise FileNotFoundError("Aucun fichier de match trouvé. Vérifiez le DATA_PATH, les noms et la plage d'années.")

    use_cols = ['tourney_id', 'tourney_date', 'surface', 'winner_id', 'winner_name', 'winner_hand', 'winner_ht','winner_age', 'loser_id', 'loser_name', 'loser_hand', 'loser_ht', 'loser_age','winner_rank', 'loser_rank', 'score','w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced','l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced']
    
    container = []
    print("\nFichiers trouvés :")
    for f in sorted(all_files):
        print(f" - {os.path.basename(f)}")
        try:
            df_cols = pd.read_csv(f, nrows=0, encoding='ISO-8859-1').columns
            cols_to_load = [col for col in use_cols if col in df_cols]
            df = pd.read_csv(f, usecols=cols_to_load, encoding='ISO-8859-1', on_bad_lines='skip', low_memory=False)
            container.append(df)
        except Exception as e:
            print(f"  Avertissement : Impossible de lire le fichier {os.path.basename(f)}. Erreur : {e}")

    if not container:
        raise ValueError("Aucun fichier n'a pu être chargé correctement.")
    
    print(f"\n{len(container)} fichiers chargés avec succès.")
    return pd.concat(container, axis=0, ignore_index=True)


# --- ÉTAPE 2 : NETTOYAGE ET OPTIMISATION DES TYPES ---
def clean_and_prepare_data(df):
    df = df.copy()
    df.dropna(subset=['score'], inplace=True)
    df = df[~df['score'].str.contains('W/O|RET|DEF|Default', na=False, case=False)].copy()

    if 'tourney_date' not in df.columns or 'tourney_id' not in df.columns:
        raise KeyError("Colonnes 'tourney_id' ou 'tourney_date' manquantes.")

    df['tourney_year'] = pd.to_numeric(df['tourney_id'].astype(str).str.split('-').str[0], errors='coerce')
    df.loc[df['tourney_date'].isna(), 'tourney_date'] = (df['tourney_year'] * 10000 + 701)
    df.dropna(subset=['tourney_date'], inplace=True)

    numeric_cols = ['winner_ht', 'winner_age', 'loser_ht', 'loser_age', 'winner_rank', 'loser_rank','w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced','l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')

    critical_cols = ['winner_rank', 'loser_rank', 'winner_id', 'loser_id', 'tourney_date', 'w_svpt', 'l_svpt']
    df.dropna(subset=critical_cols, inplace=True)
    df[['w_svpt', 'l_svpt', 'w_SvGms', 'l_SvGms', 'w_bpFaced', 'l_bpFaced']] = df[['w_svpt', 'l_svpt', 'w_SvGms', 'l_SvGms', 'w_bpFaced', 'l_bpFaced']].replace(0, 1)

    df['tourney_date'] = pd.to_datetime(df['tourney_date'], format='%Y%m%d', errors='coerce')
    df.dropna(subset=['tourney_date'], inplace=True)
    
    for col in ['winner_id', 'loser_id']: df[col] = df[col].astype('int32')
    for col in ['winner_name', 'loser_name']: df[col] = df[col].astype(str)
    
    df = df.sort_values('tourney_date').reset_index(drop=True)
    return df

# --- ÉTAPE 3 : FEATURE ENGINEERING (VERSION DÉFINITIVE CORRIGÉE) ---
def create_features_and_stats(df):
    print("\nCréation des caractéristiques et statistiques chronologiques (version définitive)...")

    # ÉTAPE A: Calculer les stats de base de service pour chaque joueur dans chaque match
    for p in ['winner', 'loser']:
        opp_prefix = 'l_' if p == 'winner' else 'w_'
        df[f'{p}_return_pts_won'] = df[f'{opp_prefix}svpt'] - df[f'{opp_prefix}1stWon'] - df[f'{opp_prefix}2ndWon']
        df[f'{p}_return_pts_total'] = df[f'{opp_prefix}svpt']
    
    # ÉTAPE B: Mettre tous les matchs sous un format unique (id_joueur, stats)
    w_df = df[['winner_id', 'tourney_date',
           'winner_rank', 'winner_age', 'winner_ht',          # <- NEW
           'w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon',
           'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced',
           'winner_return_pts_won', 'winner_return_pts_total']
         ].rename(columns=lambda c: c.replace('winner_', '').replace('w_', ''))
    w_df['won'] = 1

    l_df = df[['loser_id', 'tourney_date',
            'loser_rank', 'loser_age', 'loser_ht',              # <- NEW
            'l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon',
            'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced',
            'loser_return_pts_won', 'loser_return_pts_total']
            ].rename(columns=lambda c: c.replace('loser_', '').replace('l_', ''))
    l_df['won'] = 0
    all_player_matches = pd.concat([w_df, l_df]).sort_values(['id', 'tourney_date'])
    
    # ÉTAPE C: Calculer tous les pourcentages, Y COMPRIS celui du retour
    all_player_matches['ace_rate'] = all_player_matches['ace'] / all_player_matches['SvGms']
    all_player_matches['df_rate'] = all_player_matches['df'] / all_player_matches['SvGms']
    all_player_matches['first_serve_in_pct'] = all_player_matches['1stIn'] / all_player_matches['svpt']
    all_player_matches['first_serve_win_pct'] = all_player_matches['1stWon'] / all_player_matches['1stIn'].replace(0,1)
    all_player_matches['second_serve_win_pct'] = all_player_matches['2ndWon'] / (all_player_matches['svpt'] - all_player_matches['1stIn']).replace(0,1)
    all_player_matches['bp_saved_pct'] = all_player_matches['bpSaved'] / all_player_matches['bpFaced'].replace(0,1)
    all_player_matches['service_games_win_pct'] = 1 - ((all_player_matches['bpFaced'] - all_player_matches['bpSaved']) / all_player_matches['SvGms'])
    all_player_matches['return_points_win_pct'] = all_player_matches['return_pts_won'] / all_player_matches['return_pts_total']
    all_player_matches['break_points_converted_pct'] = (all_player_matches['bpFaced'] - all_player_matches['bpSaved']) / all_player_matches['bpFaced'].replace(0,1)
    
    # ÉTAPE D: Calculer les moyennes mobiles
    # nouveau
    # nouveau
    stats_to_roll = [f for f in config.BASE_FEATURES if f not in ['ht', 'rank', 'age']] + ['won']


    grouped = all_player_matches.groupby('id')
    for stat_name in stats_to_roll:           # plus de stat_base/stat_name
        all_player_matches[f'rolling_{stat_name}'] = (
            grouped[stat_name]
            .transform(lambda x: x.shift(1).rolling(window=config.ROLLING_WINDOW,
                                                    min_periods=config.MIN_PERIODS).mean())
            .astype('float32')
        )

    all_player_matches.rename(columns={'rolling_won': 'form'}, inplace=True)
    player_db_stats = all_player_matches.sort_values('tourney_date').drop_duplicates(subset='id', keep='last').set_index('id')
    
    ### CORRECTION DE L'ERREUR 'append' ICI ###
    names_df = pd.concat([
        df[['winner_id', 'winner_name']].rename(columns={'winner_id': 'id', 'winner_name': 'name'}),
        df[['loser_id', 'loser_name']].rename(columns={'loser_id': 'id', 'loser_name': 'name'})
    ]).drop_duplicates(subset='id').set_index('id')
    player_db = names_df.join(player_db_stats)
    
    # ÉTAPE E: Assemblage final efficace
    rolling_cols = [c for c in all_player_matches.columns if 'rolling_' in c or c == 'form']
    stats_df = all_player_matches[['id', 'tourney_date'] + rolling_cols].copy()
    
    df = df.merge(stats_df, left_on=['winner_id', 'tourney_date'], right_on=['id', 'tourney_date'], how='left').rename(columns={c:f"winner_{c}" for c in rolling_cols}).drop(columns=['id'])
    df = df.merge(stats_df, left_on=['loser_id', 'tourney_date'], right_on=['id', 'tourney_date'], how='left').rename(columns={c:f"loser_{c}" for c in rolling_cols}).drop(columns=['id'])

    for feature in config.BASE_FEATURES + ['form']:
        if feature in ['rank', 'age', 'ht']:
            p1_col, p2_col = f'winner_{feature}', f'loser_{feature}'
        elif feature == 'form':
            p1_col, p2_col = 'winner_form', 'loser_form'
        else:
            p1_col, p2_col = f'winner_rolling_{feature}', f'loser_rolling_{feature}'
        
        fill_value = df[p1_col].mean() if feature in ['age', 'ht', 'rank'] else 0.5
        df.loc[:, p1_col] = df[p1_col].fillna(fill_value)
        df.loc[:, p2_col] = df[p2_col].fillna(fill_value)
        df[f'{feature}_diff'] = (df[p1_col] - df[p2_col]).astype('float32')

    diff_cols = [f'{f}_diff' for f in config.BASE_FEATURES + ['form']]
    p1_df = df[diff_cols + ['winner_name', 'loser_name', 'winner_hand', 'loser_hand', 'surface', 'tourney_date']].rename(columns={'winner_name':'p1_name', 'loser_name':'p2_name', 'winner_hand':'p1_hand', 'loser_hand':'p2_hand'})
    p1_df['result'] = 1
    p2_df = df[diff_cols + ['winner_name', 'loser_name', 'winner_hand', 'loser_hand', 'surface', 'tourney_date']].rename(columns={'loser_name':'p1_name', 'winner_name':'p2_name', 'loser_hand':'p1_hand', 'winner_hand':'p2_hand'})
    for col in diff_cols: p2_df[col] *= -1
    p2_df['result'] = 0
    final_df = pd.concat([p1_df, p2_df], ignore_index=True)
    return final_df, player_db


# --- ÉTAPE 4 : ENTRAÎNEMENT FINAL ---
def train_final_model(df, player_db): # Ajout de player_db comme argument
    print("\n--- Phase d'entraînement du Modèle Final (sur toutes les données) ---")
    df.dropna(subset=[col for col in df.columns if '_diff' in col], inplace=True)
    df_encoded = pd.get_dummies(df, columns=config.CATEGORICAL_FEATURES, dummy_na=True)
    X = df_encoded.drop(['result', 'p1_name', 'p2_name', 'tourney_date'], axis=1)
    y = df_encoded['result']
    print(f"Entraînement sur {len(X)} lignes de données (de {config.START_YEAR} à {config.END_YEAR}).")
    model = LGBMClassifier(random_state=42, n_estimators=500, learning_rate=0.05, num_leaves=31, colsample_bytree=0.8, subsample=0.8)
    model.fit(X, y)
    print("\nModèle final entraîné avec succès.")
    print("\nSauvegarde des artefacts du modèle final...")
    joblib.dump(model, config.MODEL_PATH)
    joblib.dump(X.columns.tolist(), config.COLUMNS_PATH)
    player_db.to_pickle(config.PLAYER_DB_PATH) # Sauvegarder ici
    print(f"Modèle, colonnes et base de données sauvegardés.")
    return model, X.columns

# --- EXÉCUTION PRINCIPALE ---
if __name__ == "__main__":
    try:
        raw_data = load_and_combine_matches(config.DATA_PATH, config.START_YEAR, config.END_YEAR)
        data = clean_and_prepare_data(raw_data)
        print(f"Données nettoyées : {data.shape[0]} matchs exploitables.")
        
        featured_data, player_db = create_features_and_stats(data)
        print(f"Données transformées : {featured_data.shape[0]} lignes prêtes pour le modèle.")
        
        train_final_model(featured_data, player_db)
        
        print("\n--- Entraînement final terminé avec succès ! ---")
        print("Vous pouvez maintenant utiliser 'predict.py' pour faire des prédictions.")

    except Exception as e:
        import traceback
        print(f"\nUne erreur critique est survenue : {e}")
        traceback.print_exc()