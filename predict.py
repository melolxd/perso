# =============================================================================
# (Version 9.2 - Sans la caractéristique de Forme)
# =============================================================================

import pandas as pd
import glob
import numpy as np
import os
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

# --- ÉTAPE 1 : CHARGEMENT (inchangée) ---
def load_and_combine_matches(path, start_year, end_year):
    all_files = []
    print(f"Recherche des fichiers de {start_year} à {end_year} dans le dossier : {os.path.abspath(path)}")
    for year in range(start_year, end_year + 1):
        all_files.extend(glob.glob(os.path.join(path, f'atp_matches_{year}.csv')))
        all_files.extend(glob.glob(os.path.join(path, f'atp_matches_qual_chall_{year}.csv')))
    if not all_files:
        print("\nAvertissement : Aucun fichier de match trouvé.")
        return pd.DataFrame()
    container = [pd.read_csv(f, index_col=None, header=0, encoding='ISO-8859-1', on_bad_lines='skip') for f in all_files]
    return pd.concat(container, axis=0, ignore_index=True)

# --- ÉTAPE 2 : NETTOYAGE (inchangée) ---
def clean_and_prepare_data(df):
    df.dropna(subset=['score'], inplace=True)
    df = df[~df['score'].str.contains('W/O|RET|DEF|Default', na=False, case=False)]
    relevant_cols = [
        'tourney_name', 'surface', 'tourney_date', 'winner_id', 'winner_name', 'winner_hand', 
        'winner_ht', 'winner_age', 'loser_id', 'loser_name', 'loser_hand', 'loser_ht', 'loser_age',
        'winner_rank', 'loser_rank'
    ]
    existing_cols = [col for col in relevant_cols if col in df.columns]
    df = df[existing_cols]
    numeric_cols = ['winner_ht', 'winner_age', 'loser_ht', 'loser_age', 'winner_rank', 'loser_rank']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    critical_cols = ['winner_rank', 'loser_rank', 'winner_age', 'loser_age', 'winner_id', 'loser_id']
    df.dropna(subset=critical_cols, inplace=True)
    df['tourney_date'] = pd.to_datetime(df['tourney_date'], format='%Y%m%d')
    df = df.sort_values('tourney_date').reset_index(drop=True)
    return df

# --- ÉTAPE 3 : PRÉ-CALCUL (inchangée, la forme est calculée mais ne sera pas utilisée) ---
def precompute_advanced_stats(df):
    print("\nPré-calcul des statistiques avancées (Forme et Surface)...")
    df['match_id'] = df.index
    
    winner_df = df[['match_id', 'tourney_date', 'surface', 'winner_id']].rename(columns={'winner_id': 'player_id'})
    winner_df['won'] = 1
    loser_df = df[['match_id', 'tourney_date', 'surface', 'loser_id']].rename(columns={'loser_id': 'player_id'})
    loser_df['won'] = 0
    
    all_player_matches = pd.concat([winner_df, loser_df]).sort_values(['player_id', 'tourney_date'])
    
    all_player_matches['form'] = all_player_matches.groupby('player_id')['won'].transform(lambda x: x.shift(1).rolling(10, min_periods=3).mean())
    
    surface_stats = all_player_matches.groupby(['player_id', 'surface'])['won'].agg(['mean', 'count']).rename(columns={'mean': 'surface_win_pct', 'count': 'surface_matches'})
    
    winner_form = all_player_matches[all_player_matches['won'] == 1][['match_id', 'form']].rename(columns={'form': 'winner_form'})
    loser_form = all_player_matches[all_player_matches['won'] == 0][['match_id', 'form']].rename(columns={'form': 'loser_form'})
    
    df = df.merge(winner_form, on='match_id', how='left')
    df = df.merge(loser_form, on='match_id', how='left')
    
    return df, surface_stats

# --- ÉTAPE 4 : CRÉATION DE CARACTÉRISTIQUES (Mise à jour : 'form_diff' retirée) ---
def create_features(df, surface_stats):
    p1_stats = df[['winner_id', 'winner_name', 'winner_rank', 'winner_age', 'winner_ht', 'winner_hand', 'winner_form']].rename(columns=lambda x: x.replace('winner_', ''))
    p2_stats = df[['loser_id', 'loser_name', 'loser_rank', 'loser_age', 'loser_ht', 'loser_hand', 'loser_form']].rename(columns=lambda x: x.replace('loser_', ''))
    player_db = pd.concat([p1_stats, p2_stats]).sort_values('age').drop_duplicates(subset=['id'], keep='last').set_index('id')

    df['p1_id'], df['p2_id'] = df['winner_id'], df['loser_id']
    df['p1_rank'], df['p2_rank'] = df['winner_rank'], df['loser_rank']
    df['p1_age'], df['p2_age'] = df['winner_age'], df['loser_age']
    df['p1_form'], df['p2_form'] = df['winner_form'], df['loser_form']
    df['rank_diff'] = df['p1_rank'] - df['p2_rank']
    df['age_diff'] = df['p1_age'] - df['p2_age']
    df['result'] = 1

    # --- SECTION 2: Créer la version inversée des matchs ---
    df_inv = df.copy()
    df_inv.rename(columns={
        'p1_id': 'p2_id', 'p2_id': 'p1_id',
        'p1_rank': 'p2_rank', 'p2_rank': 'p1_rank',
        'p1_age': 'p2_age', 'p2_age': 'p1_age',
        'p1_form': 'p2_form', 'p2_form': 'p1_form'
    }, inplace=True)
    df_inv['rank_diff'] = -df_inv['rank_diff']
    df_inv['age_diff'] = -df_inv['age_diff']
    df_inv['result'] = 0

    # --- SECTION 3: Combiner et finaliser le DataFrame ---
    model_df = pd.concat([df, df_inv], ignore_index=True)

    model_df = model_df.merge(player_db[['name', 'hand', 'ht']], left_on='p1_id', right_index=True, how='left').rename(columns={'name': 'p1_name', 'hand': 'p1_hand', 'ht': 'p1_ht'})
    model_df = model_df.merge(player_db[['name', 'hand', 'ht']], left_on='p2_id', right_index=True, how='left').rename(columns={'name': 'p2_name', 'hand': 'p2_hand', 'ht': 'p2_ht'})

    model_df = model_df.merge(surface_stats.reset_index()[['player_id', 'surface', 'surface_win_pct']], left_on=['p1_id', 'surface'], right_on=['player_id', 'surface'], how='left').rename(columns={'surface_win_pct': 'p1_surface_win_pct'}).drop('player_id', axis=1)
    model_df = model_df.merge(surface_stats.reset_index()[['player_id', 'surface', 'surface_win_pct']], left_on=['p2_id', 'surface'], right_on=['player_id', 'surface'], how='left').rename(columns={'surface_win_pct': 'p2_surface_win_pct'}).drop('player_id', axis=1)

    model_df['surface_win_pct_diff'] = model_df['p1_surface_win_pct'].fillna(0.5) - model_df['p2_surface_win_pct'].fillna(0.5)
    model_df['ht_diff'] = model_df['p1_ht'].fillna(185) - model_df['p2_ht'].fillna(185)
    model_df['form_diff'] = model_df['p1_form'].fillna(0.5) - model_df['p2_form'].fillna(0.5)

    feature_cols = ['rank_diff', 'age_diff', 'ht_diff', 'surface_win_pct_diff', 'form_diff', 'p1_hand', 'p2_hand', 'surface']
    final_df = model_df[feature_cols + ['p1_name', 'p2_name', 'result']].copy()
    return final_df, player_db

# --- ÉTAPE 5 : ENTRAÎNEMENT (Mise à jour : 'form_diff' retirée) ---
def train_model(df):
    print("\n--- Phase 1 : Entraînement du Modèle (AVEC FORME) ---")
    
    df.dropna(subset=['rank_diff', 'age_diff', 'ht_diff', 'surface_win_pct_diff', 'form_diff'], inplace=True)
    
    df_encoded = pd.get_dummies(df, columns=['p1_hand', 'p2_hand', 'surface'], drop_first=True)
    X = df_encoded.drop(['result', 'p1_name', 'p2_name'], axis=1)
    y = df_encoded['result']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"Séparation des données : {len(X_train)} pour l'entraînement, {len(X_test)} pour le test.")

    model = LGBMClassifier(random_state=42)
    model.fit(X_train, y_train)
    
    accuracy = accuracy_score(y_test, model.predict(X_test))
    print(f"Performance du Modèle (Accuracy) : {accuracy * 100:.2f}%")

    joblib.dump(model, 'atp_model_lgbm_WITH_form.joblib')
    joblib.dump(X.columns, 'training_columns_lgbm_WITH_form.joblib')
    print("Modèle et colonnes sauvegardés dans 'atp_model_lgbm_WITH_form.joblib' et 'training_columns_lgbm_WITH_form.joblib'")
    
    return model, X.columns

# --- ÉTAPE 6 : PRÉDICTION (Mise à jour : 'form_diff' retirée) ---
def predict_match(model, player1_name, player2_name, surface, player_db, training_columns, p1_form_manual=0.5, p2_form_manual=0.5):
    try:
        p1 = player_db[player_db['name'] == player1_name].iloc[0]
    except IndexError:
        print(f"Avertissement : Joueur '{player1_name}' non trouvé. Utilisation de stats par défaut.")
        p1 = pd.Series({'rank': 9999, 'age': 27, 'ht': 185, 'hand': 'R', 'name': player1_name})

    try:
        p2 = player_db[player_db['name'] == player2_name].iloc[0]
    except IndexError:
        print(f"Avertissement : Joueur '{player2_name}' non trouvé. Utilisation de stats par défaut.")
        p2 = pd.Series({'rank': 9999, 'age': 27, 'ht': 185, 'hand': 'R', 'name': player2_name})

    p1_rank = p1.get('rank', 9999); p2_rank = p2.get('rank', 9999)
    p1_age = p1.get('age', 27); p2_age = p2.get('age', 27)
    p1_ht = p1.get('ht', 185); p2_ht = p2.get('ht', 185)
    p1_hand = p1.get('hand', 'R'); p2_hand = p2.get('hand', 'R')
    
    if pd.isna(p1_ht): p1_ht = 185
    if pd.isna(p2_ht): p2_ht = 185
    if pd.isna(p1_hand): p1_hand = 'R'
    if pd.isna(p2_hand): p2_hand = 'R'
    
    try: p1_surface_pct = player_db.loc[p1.name, (slice(None), surface)]['surface_win_pct'].values[0]
    except (KeyError, IndexError, AttributeError): p1_surface_pct = 0.5
    try: p2_surface_pct = player_db.loc[p2.name, (slice(None), surface)]['surface_win_pct'].values[0]
    except (KeyError, IndexError, AttributeError): p2_surface_pct = 0.5

    form_diff_manual = p1_form_manual - p2_form_manual

    match_data = {
        'rank_diff': p1_rank - p2_rank,
        'age_diff': p1_age - p2_age,
        'ht_diff': p1_ht - p2_ht,
        'surface_win_pct_diff': p1_surface_pct - p2_surface_pct,
        'form_diff': form_diff_manual,
        'p1_hand': p1_hand,
        'p2_hand': p2_hand,
        'surface': surface
    }
    match_df = pd.DataFrame([match_data])

    match_df_encoded = pd.get_dummies(match_df)

    match_df_aligned = match_df_encoded.reindex(columns=training_columns, fill_value=0)

    probability = model.predict_proba(match_df_aligned)[:, 1][0]
    
    print(f"\n--- Prédiction pour {player1_name} vs {player2_name} sur {surface} (Forme: {p1_form_manual*100:.0f}% vs {p2_form_manual*100:.0f}%) ---")
    print(f"Probabilité de victoire pour {player1_name} : {probability * 100:.2f}%")
    print(f"Probabilité de victoire pour {player2_name} : {(1 - probability) * 100:.2f}%")

# =============================================================================
# EXÉCUTION PRINCIPALE DU SCRIPT
# =============================================================================
if __name__ == "__main__":
    START_YEAR = 2021
    END_YEAR = 2024
    DATA_PATH = '.'

    raw_data = load_and_combine_matches(DATA_PATH, START_YEAR, END_YEAR)

    if not raw_data.empty:
        data = clean_and_prepare_data(raw_data)
        print(f"\nDonnées nettoyées : {data.shape[0]} matchs exploitables restants.")
        
        data_adv, surface_stats = precompute_advanced_stats(data)
        
        featured_data, player_db = create_features(data_adv, surface_stats)
        print(f"Données transformées : {featured_data.shape[0]} lignes prêtes pour le modèle.")
        
        model, training_columns = train_model(featured_data)
        
        print("\n" + "="*60)
        print("      SIMULATION DE PRÉDICTIONS AVEC LE MODÈLE 'LGBM' (SANS FORME)")
        print("="*60)

        predict_match(model=model, player1_name="Emilio Nava", player2_name="Ugo Humbert", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.6, p2_form_manual=0.4)
        predict_match(model=model, player1_name="Karen Khachanov", player2_name="Juan Pablo Ficovich", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.8, p2_form_manual=0.8)
        predict_match(model=model, player1_name="Alexandre Muller", player2_name="Miomir Kecmanovic", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.2, p2_form_manual=0.6)
        predict_match(model=model, player1_name="Giovanni Mpetshi Perricard", player2_name="Holger Rune", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.2, p2_form_manual=0.4)
        predict_match(model=model, player1_name="Tomas Barrios Vera", player2_name="Alex Michelsen", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.4, p2_form_manual=0.4)
        predict_match(model=model, player1_name="Mikael Arseneault", player2_name="Alexei Popyrin", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.6, p2_form_manual=0.2)
        predict_match(model=model, player1_name="Jenson Brooksby", player2_name="Corentin Moutet", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.4, p2_form_manual=0.6)
        predict_match(model=model, player1_name="Tallon Griekspoor", player2_name="Tomas Martin Etcheverry", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.6, p2_form_manual=0.2)
        predict_match(model=model, player1_name="Jaume Munar", player2_name="Francisco Cerundolo", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.6, p2_form_manual=0.4)
        predict_match(model=model, player1_name="Nuno Borges", player2_name="Facundo Bagnis", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.4, p2_form_manual=0.8)
        predict_match(model=model, player1_name="Reilly Opelka", player2_name="Tomas Machac", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.6, p2_form_manual=0.6)
        predict_match(model=model, player1_name="Tristan Schoolkate", player2_name="Matteo Arnaldi", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.8, p2_form_manual=0.4)
        predict_match(model=model, player1_name="Roman Safiullin", player2_name="Casper Ruud", surface="Hard", player_db=player_db, training_columns=training_columns, p1_form_manual=0.4, p2_form_manual=0.4)