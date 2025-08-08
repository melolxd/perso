# train.py — version corrigée complète

import pandas as pd
import numpy as np
import glob
import os
import joblib
from lightgbm import LGBMClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import log_loss
import optuna
import warnings

import config

warnings.filterwarnings("ignore", category=optuna.exceptions.ExperimentalWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)


# -----------------------------
# ÉTAPE 1 : CHARGEMENT
# -----------------------------
def load_and_combine_matches(path, start_year, end_year):
    all_files = []
    print(f"Recherche des fichiers de {start_year} à {end_year} dans le dossier : {os.path.abspath(path)}")
    for year in range(start_year, end_year + 1):
        all_files.extend(glob.glob(os.path.join(path, f'atp_matches_{year}.csv')))
        all_files.extend(glob.glob(os.path.join(path, f'atp_matches_qual_chall_{year}.csv')))

    if not all_files:
        raise FileNotFoundError("Aucun fichier de match trouvé. Vérifiez le DATA_PATH, les noms et la plage d'années.")

    use_cols = [
        'tourney_id', 'tourney_date', 'surface',
        'winner_id', 'winner_name', 'winner_hand', 'winner_ht', 'winner_age',
        'loser_id', 'loser_name', 'loser_hand', 'loser_ht', 'loser_age',
        'winner_rank', 'loser_rank', 'score',
        'w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced',
        'l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced'
    ]
    
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


# -----------------------------
# ÉTAPE 2 : NETTOYAGE
# -----------------------------
def clean_and_prepare_data(df):
    df = df.copy()
    df.dropna(subset=['score'], inplace=True)
    df = df[~df['score'].str.contains('W/O|RET|DEF|Default', na=False, case=False)].copy()

    if 'tourney_date' not in df.columns or 'tourney_id' not in df.columns:
        raise KeyError("Colonnes 'tourney_id' ou 'tourney_date' manquantes.")

    df['tourney_year'] = pd.to_numeric(df['tourney_id'].astype(str).str.split('-').str[0], errors='coerce')
    df.loc[df['tourney_date'].isna(), 'tourney_date'] = (df['tourney_year'] * 10000 + 701)
    df.dropna(subset=['tourney_date'], inplace=True)

    numeric_cols = [
        'winner_ht', 'winner_age', 'loser_ht', 'loser_age', 'winner_rank', 'loser_rank',
        'w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced',
        'l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')

    critical_cols = ['winner_rank', 'loser_rank', 'winner_id', 'loser_id', 'tourney_date', 'w_svpt', 'l_svpt']
    df.dropna(subset=critical_cols, inplace=True)
    
    # Remplacer 0 par 1 pour éviter div/0
    div_cols = ['w_svpt', 'l_svpt', 'w_SvGms', 'l_SvGms', 'w_bpFaced', 'l_bpFaced', 'w_1stIn', 'l_1stIn']
    for col in div_cols:
        if col in df.columns:
            df[col] = df[col].replace(0, 1)

    df['tourney_date'] = pd.to_datetime(df['tourney_date'], format='%Y%m%d', errors='coerce')
    df.dropna(subset=['tourney_date'], inplace=True)
    
    for col in ['winner_id', 'loser_id']:
        df[col] = df[col].astype('int32')
    for col in ['winner_name', 'loser_name']:
        df[col] = df[col].astype(str)
    
    df = df.sort_values('tourney_date').reset_index(drop=True)
    return df


# -----------------------------
# ÉTAPE 3 : FEATURES
# -----------------------------
def calculate_elo(df):
    print("Calcul des classements Elo...")
    elos = {}
    winner_elos, loser_elos = [], []
    
    for _, row in df.iterrows():
        w_id, l_id = row['winner_id'], row['loser_id']
        w_elo = elos.get(w_id, config.ELO_DEFAULT)
        l_elo = elos.get(l_id, config.ELO_DEFAULT)
        winner_elos.append(w_elo)
        loser_elos.append(l_elo)
        # update
        expected_win = 1 / (1 + 10 ** ((l_elo - w_elo) / 400))
        elo_change = config.ELO_K_FACTOR * (1 - expected_win)
        elos[w_id] = w_elo + elo_change
        elos[l_id] = l_elo - elo_change
        
    df['winner_elo'] = winner_elos
    df['loser_elo'] = loser_elos
    return df

def calculate_h2h(df):
    print("Calcul des confrontations directes (H2H)...")
    h2h = {}
    w_h2h, l_h2h = [], []

    for _, row in df.iterrows():
        w_id, l_id = row['winner_id'], row['loser_id']
        p1, p2 = tuple(sorted((w_id, l_id)))
        wins1, wins2 = h2h.get((p1, p2), (0, 0))
        if w_id == p1:
            w_h2h.append(wins1 / (wins1 + wins2) if (wins1 + wins2) > 0 else 0.5)
            l_h2h.append(wins2 / (wins1 + wins2) if (wins1 + wins2) > 0 else 0.5)
            h2h[(p1, p2)] = (wins1 + 1, wins2)
        else:
            w_h2h.append(wins2 / (wins1 + wins2) if (wins1 + wins2) > 0 else 0.5)
            l_h2h.append(wins1 / (wins1 + wins2) if (wins1 + wins2) > 0 else 0.5)
            h2h[(p1, p2)] = (wins1, wins2 + 1)
    df['winner_h2h_win_pct'] = w_h2h
    df['loser_h2h_win_pct'] = l_h2h
    return df

def create_features_and_stats(df):
    print("\nCréation des caractéristiques et statistiques (version optimisée)...")
    # Elo / H2H
    df = calculate_elo(df)
    df = calculate_h2h(df)

    # A. Stats de retour
    for p in ['winner', 'loser']:
        opp = 'l_' if p == 'winner' else 'w_'
        df[f'{p}_return_pts_won'] = df[f'{opp}svpt'] - df[f'{opp}1stWon'] - df[f'{opp}2ndWon']
        df[f'{p}_return_pts_total'] = df[f'{opp}svpt']

    # B. Flatten par joueur (exclure *name* pour éviter conflits)
    w_cols_keys = [c for c in df.columns if (c.startswith('winner_') or c.startswith('w_')) and c != 'winner_name']
    l_cols_keys = [c for c in df.columns if (c.startswith('loser_') or c.startswith('l_')) and c != 'loser_name']

    w_cols = {c: c.replace('winner_', '').replace('w_', '') for c in w_cols_keys}
    l_cols = {c: c.replace('loser_', '').replace('l_', '') for c in l_cols_keys}

    common_cols = ['tourney_date', 'surface']
    w_df = df[w_cols_keys + common_cols].rename(columns=w_cols)
    l_df = df[l_cols_keys + common_cols].rename(columns=l_cols)
    # sécurité : pas de name ici
    w_df = w_df.drop(columns=['name'], errors='ignore')
    l_df = l_df.drop(columns=['name'], errors='ignore')

    w_df['won'] = 1
    l_df['won'] = 0
    all_player_matches = pd.concat([w_df, l_df]).sort_values(['id', 'tourney_date'])

    # C. Ratios/Pourcentages
    def safe_div(a, b):
        return (a / b.replace(0, 1)).astype('float32')

    for stat in config.DERIVED_STATS:
        if stat == 'ace_rate':
            all_player_matches[stat] = safe_div(all_player_matches['ace'], all_player_matches['SvGms'])
        elif stat == 'df_rate':
            all_player_matches[stat] = safe_div(all_player_matches['df'], all_player_matches['SvGms'])
        elif stat == 'first_serve_in_pct':
            all_player_matches[stat] = safe_div(all_player_matches['1stIn'], all_player_matches['svpt'])
        elif stat == 'first_serve_win_pct':
            all_player_matches[stat] = safe_div(all_player_matches['1stWon'], all_player_matches['1stIn'])
        elif stat == 'second_serve_win_pct':
            all_player_matches[stat] = safe_div(all_player_matches['2ndWon'], (all_player_matches['svpt'] - all_player_matches['1stIn']).replace(0, 1))
        elif stat == 'bp_saved_pct':
            all_player_matches[stat] = safe_div(all_player_matches['bpSaved'], all_player_matches['bpFaced'])
        elif stat == 'service_games_win_pct':
            all_player_matches[stat] = (1 - safe_div((all_player_matches['bpFaced'] - all_player_matches['bpSaved']), all_player_matches['SvGms']))
        elif stat == 'return_points_win_pct':
            all_player_matches[stat] = safe_div(all_player_matches['return_pts_won'], all_player_matches['return_pts_total'])

    # D. Rolling (global + par surface)
    stats_to_roll = config.DERIVED_STATS + ['won']
    grouped_player = all_player_matches.groupby('id', sort=False)
    for stat in stats_to_roll:
        all_player_matches[f'rolling_{stat}'] = (
            grouped_player[stat].transform(lambda x: x.shift(1).rolling(config.ROLLING_WINDOW, min_periods=config.MIN_PERIODS).mean())
        ).astype('float32')

    grouped_surface = all_player_matches.groupby(['id', 'surface'], sort=False)
    for stat in stats_to_roll:
        all_player_matches[f'rolling_{stat}_surface'] = (
            grouped_surface[stat].transform(lambda x: x.shift(1).rolling(config.ROLLING_WINDOW, min_periods=config.MIN_PERIODS).mean())
        ).astype('float32')

    all_player_matches.rename(columns={'rolling_won': 'form', 'rolling_won_surface': 'form_surface'}, inplace=True)

    # E. Merge back
    rolling_cols = [c for c in all_player_matches.columns if c.startswith('rolling_') or c in ('form', 'form_surface')]
    stats_df = all_player_matches[['id', 'tourney_date'] + rolling_cols].copy()

    df = df.merge(
        stats_df, left_on=['winner_id', 'tourney_date'], right_on=['id', 'tourney_date'], how='left'
    ).rename(columns={c: f"winner_{c}" for c in rolling_cols}).drop(columns=['id'])
    df = df.merge(
        stats_df, left_on=['loser_id', 'tourney_date'], right_on=['id', 'tourney_date'], how='left'
    ).rename(columns={c: f"loser_{c}" for c in rolling_cols}).drop(columns=['id'])

    # F. Features différentielles
    feature_map = {
        **{f: f'winner_rolling_{f}' for f in config.DERIVED_STATS},
        **{f: f'winner_rolling_{f}_surface' for f in config.DERIVED_STATS},
        **{f: f'winner_{f}' for f in config.PLAYER_INFO + config.COMPOSITE_FEATURES},
        'form': 'winner_form',
        'form_surface': 'winner_form_surface'
    }
    for feature, p1_col_template in feature_map.items():
        p2_col_template = p1_col_template.replace('winner', 'loser')
        fill_val = 0.5 if any(k in feature for k in ['pct', 'rate', 'form']) else df[p1_col_template].mean()
        df[p1_col_template] = df[p1_col_template].fillna(fill_val)
        df[p2_col_template] = df[p2_col_template].fillna(fill_val)
        df[f'{feature}_diff'] = (df[p1_col_template] - df[p2_col_template]).astype('float32')
        if '_surface' in p1_col_template:
            base = feature.replace('_surface', '')
            df[f'{base}_surface_diff'] = (df[p1_col_template] - df[p2_col_template]).astype('float32')

    # G. Symétrisation
    diff_cols = [c for c in df.columns if c.endswith('_diff')]
    p1_df = df[diff_cols + ['winner_name', 'loser_name', 'winner_hand', 'loser_hand', 'surface', 'tourney_date']].rename(
        columns={'winner_name': 'p1_name', 'loser_name': 'p2_name', 'winner_hand': 'p1_hand', 'loser_hand': 'p2_hand'}
    )
    p1_df['result'] = 1

    p2_df = df[diff_cols + ['winner_name', 'loser_name', 'winner_hand', 'loser_hand', 'surface', 'tourney_date']].rename(
        columns={'loser_name': 'p1_name', 'winner_name': 'p2_name', 'loser_hand': 'p1_hand', 'winner_hand': 'p2_hand'}
    )
    for col in diff_cols:
        p2_df[col] *= -1
    p2_df['result'] = 0

    final_df = pd.concat([p1_df, p2_df], ignore_index=True).sort_values('tourney_date').reset_index(drop=True)

    # Base joueurs pour l'inférence
    player_db_stats = (
        all_player_matches
        .sort_values('tourney_date')
        .drop_duplicates(subset='id', keep='last')
    )
    # IMPORTANT : enlever toute colonne 'name' ici
    player_db_stats = player_db_stats.drop(columns=['name'], errors='ignore').set_index('id')

    names_df = pd.concat([
        df[['winner_id', 'winner_name']].rename(columns={'winner_id': 'id', 'winner_name': 'name'}),
        df[['loser_id', 'loser_name']].rename(columns={'loser_id': 'id', 'loser_name': 'name'})
    ]).drop_duplicates(subset='id').set_index('id')

    player_db = names_df.join(player_db_stats, how='left')

    return final_df, player_db


# -----------------------------
# ÉTAPE 4 : OPTUNA + TRAIN
# -----------------------------
def train_with_tuning(df):
    print("\n--- Phase d'optimisation des hyperparamètres avec Optuna ---")
    df = df.copy()
    df.dropna(subset=[col for col in df.columns if col.endswith('_diff')], inplace=True)
    df_encoded = pd.get_dummies(df, columns=config.CATEGORICAL_FEATURES, dummy_na=False)

    X = df_encoded.drop(columns=['result', 'p1_name', 'p2_name', 'tourney_date'])
    y = df_encoded['result']

    def objective(trial):
        params = {
            'objective': 'binary',
            'metric': 'logloss',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'random_state': 42,
            'n_estimators': 1000,
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'num_leaves': trial.suggest_int('num_leaves', 20, 100),
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
        }
        tscv = TimeSeriesSplit(n_splits=5)
        scores = []
        for train_index, val_index in tscv.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]
            model = LGBMClassifier(**params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], eval_metric='logloss')
            preds = model.predict_proba(X_val)[:, 1]
            scores.append(log_loss(y_val, preds))
        return float(np.mean(scores))

    study = optuna.create_study(direction='minimize', pruner=optuna.pruners.MedianPruner())
    study.optimize(objective, n_trials=50, timeout=600)

    print("\nOptimisation terminée.")
    print(f"Meilleur score (logloss) : {study.best_value:.4f}")
    print("Meilleurs hyperparamètres :")
    for k, v in study.best_params.items():
        print(f"  - {k}: {v}")

    print("\n--- Entraînement du modèle final sur toutes les données ---")
    final_params = dict(study.best_params)
    final_params.update({'random_state': 42, 'n_estimators': 1500})
    final_model = LGBMClassifier(**final_params)
    final_model.fit(X, y)

    print("\nSauvegarde des artefacts du modèle final...")
    os.makedirs(os.path.dirname(config.MODEL_PATH), exist_ok=True)
    joblib.dump(final_model, config.MODEL_PATH)
    joblib.dump(X.columns.tolist(), config.COLUMNS_PATH)

    return final_model, X.columns


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    try:
        raw_data = load_and_combine_matches(config.DATA_PATH, config.START_YEAR, config.END_YEAR)
        data = clean_and_prepare_data(raw_data)
        print(f"Données nettoyées : {data.shape[0]} matchs exploitables.")

        featured_data, player_db = create_features_and_stats(data)
        print(f"Données transformées : {featured_data.shape[0]} lignes prêtes pour le modèle.")

        # Sauvegarde base joueurs
        os.makedirs(os.path.dirname(config.PLAYER_DB_PATH), exist_ok=True)
        player_db.to_pickle(config.PLAYER_DB_PATH)
        print(f"Base de données joueurs sauvegardée dans {config.PLAYER_DB_PATH}")

        model, columns = train_with_tuning(featured_data)

        print("\n--- Entraînement optimisé terminé avec succès ! ---")
        print("Vous pouvez maintenant utiliser votre application Flask pour les prédictions.")

    except Exception as e:
        import traceback
        print(f"\nUne erreur critique est survenue : {e}")
        traceback.print_exc()
