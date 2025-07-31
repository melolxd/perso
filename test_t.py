import time
import os
import re
import pandas as pd
from datetime import datetime
import traceback
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import random # Importer le module pour les pauses aléatoires

# =============================================================================
# FONCTION 2 : Le Plongeur (Mise à jour pour gérer Cloudflare)
# =============================================================================

def get_detailed_stats(stats_url, driver):
    match_id = stats_url.split('/')[-2]
    print(f"      -> Plongée pour les stats : {match_id}")
    stats_dict = {}
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            driver.get(stats_url)
            time.sleep(random.uniform(2, 4))  # Pause initiale
            
            # *** GESTION CLOUDFLARE AMÉLIORÉE ***
            cloudflare_handled = False
            cloudflare_attempts = 0
            max_cloudflare_attempts = 5
            
            while cloudflare_attempts < max_cloudflare_attempts:
                try:
                    # Vérifier si on est dans une page de challenge Cloudflare
                    page_title = driver.title.lower()
                    page_source = driver.page_source.lower()
                    
                    # Indicateurs de présence de Cloudflare
                    cloudflare_indicators = [
                        'just a moment' in page_title,
                        'checking your browser' in page_source,
                        'cloudflare' in page_source,
                        'challenge-running' in page_source,
                        'cf-spinner-allow' in page_source
                    ]
                    
                    if any(cloudflare_indicators):
                        print(f"        -> Challenge Cloudflare détecté (tentative {cloudflare_attempts + 1})")
                        
                        # Attendre que la page se charge complètement
                        time.sleep(random.uniform(3, 6))
                        
                        # Méthode 1: Chercher l'iframe de challenge
                        try:
                            iframe_selectors = [
                                "iframe[src*='challenges.cloudflare.com']",
                                "iframe[title*='Widget containing']",
                                "iframe[data-hcaptcha-widget-id]",
                                "iframe#cf-chl-widget"
                            ]
                            
                            iframe_found = False
                            for selector in iframe_selectors:
                                try:
                                    iframe_locator = (By.CSS_SELECTOR, selector)
                                    WebDriverWait(driver, 5).until(
                                        EC.frame_to_be_available_and_switch_to_it(iframe_locator)
                                    )
                                    iframe_found = True
                                    print(f"        -> Iframe trouvée avec sélecteur: {selector}")
                                    break
                                except TimeoutException:
                                    continue
                            
                            if iframe_found:
                                # Chercher et cliquer la checkbox dans l'iframe
                                checkbox_selectors = [
                                    "input[type='checkbox']",
                                    "label input",
                                    ".cb-i",
                                    "#challenge-form input[type='checkbox']"
                                ]
                                
                                checkbox_clicked = False
                                for cb_selector in checkbox_selectors:
                                    try:
                                        checkbox = WebDriverWait(driver, 8).until(
                                            EC.element_to_be_clickable((By.CSS_SELECTOR, cb_selector))
                                        )
                                        
                                        # Essayer plusieurs méthodes de clic
                                        try:
                                            checkbox.click()
                                        except:
                                            driver.execute_script("arguments[0].click();", checkbox)
                                        
                                        print(f"        -> Checkbox cliquée avec succès: {cb_selector}")
                                        checkbox_clicked = True
                                        break
                                    except:
                                        continue
                                
                                # Revenir au contenu principal
                                driver.switch_to.default_content()
                                
                                if checkbox_clicked:
                                    # Attendre que le challenge soit résolu
                                    wait_time = random.uniform(8, 15)
                                    print(f"        -> Attente de {wait_time:.1f}s pour la validation...")
                                    time.sleep(wait_time)
                                    cloudflare_handled = True
                                    break
                                else:
                                    print("        -> Impossible de cliquer la checkbox")
                            else:
                                # Méthode 2: Attente passive pour challenge automatique
                                print("        -> Pas d'iframe trouvée, attente passive...")
                                time.sleep(random.uniform(10, 15))
                        
                        except Exception as e:
                            print(f"        -> Erreur dans la gestion de l'iframe: {e}")
                            driver.switch_to.default_content()  # Sécurité
                        
                        # Méthode 3: Attendre que la page change
                        try:
                            WebDriverWait(driver, 15).until(
                                lambda d: 'just a moment' not in d.title.lower() and 
                                         'checking your browser' not in d.page_source.lower()
                            )
                            print("        -> Page changée, challenge probablement résolu")
                            cloudflare_handled = True
                            break
                        except TimeoutException:
                            print("        -> Timeout en attendant la résolution du challenge")
                    
                    else:
                        # Pas de challenge détecté
                        cloudflare_handled = True
                        break
                
                except Exception as e:
                    print(f"        -> Erreur lors de la vérification Cloudflare: {e}")
                    driver.switch_to.default_content()
                
                cloudflare_attempts += 1
                if cloudflare_attempts < max_cloudflare_attempts:
                    time.sleep(random.uniform(5, 10))
            
            if not cloudflare_handled:
                print("        -> Impossible de résoudre le challenge Cloudflare")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"        -> Nouvelle tentative ({retry_count + 1}/{max_retries}) dans 30s...")
                    time.sleep(30)
                    continue
                else:
                    return {}
            
            # *** EXTRACTION DES STATS ***
            try:
                # Attendre que la page des stats soit chargée
                WebDriverWait(driver, 20).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.ID, "MatchStats")),
                        EC.presence_of_element_located((By.CLASS_NAME, "statTileWrapper"))
                    )
                )
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                stats_container = soup.find('div', id='MatchStats')
                if not stats_container:
                    print("        -> ERREUR: Conteneur de stats principal 'MatchStats' non trouvé.")
                    return {}

                stat_tiles = stats_container.find_all('div', class_='statTileWrapper')
                
                if not stat_tiles:
                    print("        -> ERREUR: Aucune tuile de stats trouvée.")
                    return {}

                for tile in stat_tiles:
                    stat_name_elem = tile.find('div', class_='labelWrappper')
                    if not stat_name_elem: 
                        continue
                    stat_name = stat_name_elem.get_text(strip=True)

                    p1_val_elem = tile.select_one('.p1Stats .non-speed')
                    p2_val_elem = tile.select_one('.p2Stats .non-speed')

                    if p1_val_elem and p2_val_elem:
                        p1_val = p1_val_elem.get_text(strip=True)
                        p2_val = p2_val_elem.get_text(strip=True)
                        stats_dict[stat_name] = (p1_val, p2_val)
                
                if stats_dict:
                    print(f"        -> {len(stats_dict)} statistiques extraites avec succès")
                    return stats_dict
                else:
                    print("        -> Aucune statistique extraite")
                    return {}
                
            except TimeoutException:
                print("        -> Timeout lors du chargement de la section stats")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"        -> Nouvelle tentative ({retry_count + 1}/{max_retries})...")
                    time.sleep(random.uniform(10, 20))
                    continue
                else:
                    return {}
        
        except Exception as e:
            print(f"        -> Erreur générale lors de la plongée [{match_id}]: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"        -> Nouvelle tentative ({retry_count + 1}/{max_retries})...")
                time.sleep(random.uniform(15, 30))
                continue
            else:
                return {}
    
    print(f"        -> Échec définitif après {max_retries} tentatives pour [{match_id}]")
    return {}

# =============================================================================
# Fonction d'Aide (Ne change pas)
# =============================================================================
def parse_stat_value(text, index=0):
    if not text:
        return 0
    numbers = re.findall(r'\d+', text)
    if numbers and len(numbers) > index:
        return int(numbers[index])
    return 0
    
# =============================================================================
# FONCTION 3 : L'Ouvrier et Transformateur (Mise à jour avec pause aléatoire)
# =============================================================================
def scrape_and_process_tournament(tourney_info, year, driver):
    tourney_name = tourney_info['name'].replace('-', ' ').title()
    tourney_id = tourney_info['id']
    url = f"https://www.atptour.com/en/scores/archive/{tourney_info['name']}/{tourney_id}/{year}/results"
    
    print(f"\n  Scraping de : {tourney_name} ({year})")
    try:
        driver.get(url)
        try:
            cookie_button_locator = (By.ID, "onetrust-accept-btn-handler")
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(cookie_button_locator)
            )
            time.sleep(1.5) 
            button = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            driver.execute_script("arguments[0].click();", button)
            print("    -> Cookies acceptés via JavaScript.")
            time.sleep(1.5)
        except Exception:
            print("    -> Pas de bandeau de cookies trouvé ou erreur lors du clic.")
        
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "atp_scores-results")))
        print("    -> Page de résultats chargée.")

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Le reste du parsing initial reste le même
        # ...
        date_location_div = soup.find('div', class_='date-location')
        tourney_date, surface, draw_size, tourney_level = "", "Hard", 32, "A"
        if date_location_div:
            date_span = date_location_div.find_all('span')[1]
            full_date_string = date_span.get_text(strip=True).split(',')[0]
            start_day_str = full_date_string.split('-')[0].strip()
            month_str = ''.join(filter(str.isalpha, full_date_string))
            tourney_date_str = f"{start_day_str} {month_str}"
            tourney_date = datetime.strptime(f"{tourney_date_str} {year}", "%d %b %Y").strftime('%Y%m%d')
        
        all_formatted_matches = []
        match_blocks = soup.select('div.atp_scores-results div.match')
        match_num_counter = len(match_blocks) + 1
        
        print(f"    -> {len(match_blocks)} blocs de match trouvés.")

        for match_block in match_blocks:
            # Ajout d'une pause aléatoire pour paraître plus humain
            time.sleep(random.uniform(1.5, 3.5))

            match_num_counter -= 1
            note_text = (match_block.find('div', class_='match-notes') or BeautifulSoup('', 'html.parser')).get_text().lower()
            if 'walkover' in note_text: 
                print(f"      -> Match {match_num_counter} ignoré (walkover).")
                continue

            # ... (Le reste du parsing des infos du match reste identique)
            # ...
            header = match_block.find('div', 'match-header').find('span').get_text(strip=True)
            round_name_map = {"Final": "F", "Semifinals": "SF", "Quarterfinals": "QF", "Round of 16": "R16", "Round of 32": "R32", "2nd Round Qualifying": "Q2", "1st Round Qualifying": "Q1"}
            round_code = round_name_map.get(header.split('.')[0].strip(), "Unknown")
            try: 
                time_str = match_block.select_one('.match-header span:last-of-type').get_text(strip=True)
                h, m, s = map(int, time_str.split(':'))
                minutes = h * 60 + m
            except: minutes = 0
            stats_items = match_block.find_all('div', 'stats-item')
            if len(stats_items) < 2: continue
            winner_info = stats_items[0].find('div', 'player-info')
            loser_info = stats_items[1].find('div', 'player-info')
            winner_name = winner_info.find('div', 'name').find('a').get_text(strip=True)
            winner_id = (winner_info.find('a')['href'].split('/')[3])
            winner_ioc = (winner_info.find('svg', 'atp-flag').find('use')['href'].split('-')[-1].upper())
            loser_name = loser_info.find('div', 'name').find('a').get_text(strip=True)
            loser_id = (loser_info.find('a')['href'].split('/')[3])
            loser_ioc = (loser_info.find('svg', 'atp-flag').find('use')['href'].split('-')[-1].upper())
            winner_seed, winner_entry = ((winner_info.find('div', 'name').find('span') or BeautifulSoup('', 'html.parser')).get_text(strip=True).strip('()'), '')
            if winner_seed.upper() in ['WC', 'Q', 'LL']: winner_entry, winner_seed = winner_seed.upper(), ''
            loser_seed, loser_entry = ((loser_info.find('div', 'name').find('span') or BeautifulSoup('', 'html.parser')).get_text(strip=True).strip('()'), '')
            if loser_seed.upper() in ['WC', 'Q', 'LL']: loser_entry, loser_seed = loser_seed.upper(), ''
            score_text_container = match_block.find('div', 'match-notes')
            score = " ".join(score_text_container.get_text(strip=True).replace("Game Set and Match", "").split(".")[1].strip().split()) if score_text_container else "N/A"
            if "wins the match" in score: score = score.split("wins the match")[1].strip()

            match_data = {'tourney_id': f"{year}-{tourney_id}", 'tourney_name': tourney_name, 'surface': surface, 'draw_size': draw_size, 'tourney_level': tourney_level, 'tourney_date': tourney_date, 'match_num': match_num_counter, 'winner_id': winner_id, 'winner_seed': winner_seed, 'winner_entry': winner_entry, 'winner_name': winner_name, 'winner_hand': 'R', 'winner_ht': 0, 'winner_ioc': winner_ioc, 'winner_age': 0.0, 'loser_id': loser_id, 'loser_seed': loser_seed, 'loser_entry': loser_entry, 'loser_name': loser_name, 'loser_hand': 'R', 'loser_ht': 0, 'loser_ioc': loser_ioc, 'loser_age': 0.0, 'score': score, 'best_of': 3, 'round': round_code, 'minutes': minutes}
            
            stats_dict = {}
            stats_link_tag = match_block.find('a', href=re.compile(r'/en/scores/stats-centre/archive/'))
            if stats_link_tag:
                stats_url = 'https://www.atptour.com' + stats_link_tag['href']
                stats_dict = get_detailed_stats(stats_url, driver)
            else:
                 print(f"      -> Pas de lien de stats trouvé pour le match {match_num_counter}.")
            
            w_val, l_val = stats_dict.get('Aces', ('0', '0'))
            match_data['w_ace'] = parse_stat_value(w_val)
            match_data['l_ace'] = parse_stat_value(l_val)
            w_val, l_val = stats_dict.get('Double Faults', ('0', '0'))
            match_data['w_df'] = parse_stat_value(w_val)
            match_data['l_df'] = parse_stat_value(l_val)
            w_val, l_val = stats_dict.get('First serve', ('0/0', '0/0'))
            match_data['w_1stIn'] = parse_stat_value(w_val, 0)
            match_data['w_svpt'] = parse_stat_value(w_val, 1)
            match_data['l_1stIn'] = parse_stat_value(l_val, 0)
            match_data['l_svpt'] = parse_stat_value(l_val, 1)
            w_val, l_val = stats_dict.get('1st serve points won', ('0/0', '0/0'))
            match_data['w_1stWon'] = parse_stat_value(w_val, 0)
            match_data['l_1stWon'] = parse_stat_value(l_val, 0)
            w_val, l_val = stats_dict.get('2nd serve points won', ('0/0', '0/0'))
            match_data['w_2ndWon'] = parse_stat_value(w_val, 0)
            match_data['l_2ndWon'] = parse_stat_value(l_val, 0)
            w_val, l_val = stats_dict.get('Break Points Saved', ('0/0', '0/0'))
            match_data['w_bpSaved'] = parse_stat_value(w_val, 0)
            match_data['w_bpFaced'] = parse_stat_value(w_val, 1)
            match_data['l_bpSaved'] = parse_stat_value(l_val, 0)
            match_data['l_bpFaced'] = parse_stat_value(l_val, 1)
            w_val, l_val = stats_dict.get('Service Games Played', ('0', '0'))
            match_data['w_SvGms'] = parse_stat_value(w_val)
            match_data['l_SvGms'] = parse_stat_value(l_val)
            match_data.update({f'{prefix}_{stat}': 0.0 for prefix in ['winner', 'loser'] for stat in ['rank', 'rank_points']})
            all_formatted_matches.append(match_data)
        
        print(f"    -> {len(all_formatted_matches)} matchs valides extraits et traités.")
        return all_formatted_matches
        
    except Exception as e:
        print(f"  -- ERREUR CRITIQUE lors du traitement de {tourney_name}.")
        traceback.print_exc()
        return []

def setup_stealth_browser():
    """Configuration avancée pour minimiser la détection Cloudflare"""
    
    options = uc.ChromeOptions()
    
    # Options de base
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Headers et User-Agent réalistes
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Options pour réduire la détection
    options.add_argument("--disable-extensions-file-access-check")
    options.add_argument("--disable-extensions-http-throttling")
    options.add_argument("--disable-extensions-except")
    options.add_argument("--disable-plugins-discovery")
    options.add_argument("--disable-preconnect")
    
    # Simulation d'un navigateur normal
    options.add_argument("--enable-features=NetworkService,NetworkServiceLogging")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-ipc-flooding-protection")
    
    # Mémoire et performance
    options.add_argument("--max_old_space_size=4096")
    options.add_argument("--disable-dev-shm-usage")
    
    try:
        driver = uc.Chrome(
            options=options, 
            use_subprocess=True,
            version_main=None  # Utilise la version détectée automatiquement
        )
        
        # Scripts pour masquer l'automatisation
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        driver.execute_script("window.chrome = { runtime: {} }")
        
        return driver
        
    except Exception as e:
        print(f"Erreur lors de l'initialisation du navigateur: {e}")
        # Fallback avec configuration basique
        return uc.Chrome(options=options, use_subprocess=True)

# Fonction d'attente intelligente
def smart_wait(driver, min_time=2, max_time=5):
    """Attente avec variation aléatoire pour simuler un comportement humain"""
    wait_time = random.uniform(min_time, max_time)
    time.sleep(wait_time)
    
    # Parfois, faire un petit mouvement de souris pour simuler l'activité
    if random.random() < 0.3:  # 30% de chance
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            action = ActionChains(driver)
            action.move_by_offset(random.randint(-10, 10), random.randint(-10, 10))
            action.perform()
        except:
            pass  # Ignorer si ça échoue

if __name__ == "__main__":
    TARGET_TOURNAMENT_INFO = {'name': 'adelaide', 'id': '8998'}
    YEAR_TO_SCRAPE = 2025
    driver = None
    try:
        print("--- Initialisation du navigateur FURTIF ---")
        options = uc.ChromeOptions()
        options.add_argument("--start-maximized")
        driver = uc.Chrome(options=options, use_subprocess=True)
        
        processed_matches = scrape_and_process_tournament(TARGET_TOURNAMENT_INFO, YEAR_TO_SCRAPE, driver)
        print("\n--- Scraping terminé ---")
        
        if processed_matches:
            header = ['tourney_id', 'tourney_name', 'surface', 'draw_size', 'tourney_level', 'tourney_date', 'match_num', 'winner_id', 'winner_seed', 'winner_entry', 'winner_name', 'winner_hand', 'winner_ht', 'winner_ioc', 'winner_age', 'loser_id', 'loser_seed', 'loser_entry', 'loser_name', 'loser_hand', 'loser_ht', 'loser_ioc', 'loser_age', 'score', 'best_of', 'round', 'minutes', 'w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced', 'winner_rank', 'winner_rank_points', 'loser_rank', 'loser_rank_points']
            df_final = pd.DataFrame(processed_matches, columns=header).fillna(0)
            
            int_cols = ['minutes', 'w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced']
            for col in int_cols:
                if col in df_final.columns:
                    df_final[col] = df_final[col].astype(int)

            filename = f"atp_matches_{YEAR_TO_SCRAPE}_{TARGET_TOURNAMENT_INFO['name'].upper()}_COMPLET.csv"
            df_final.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\nFichier '{filename}' créé avec succès, contenant les statistiques détaillées.")
        else:
            print("Échec : Aucun match n'a pu être scrapé.")
    except Exception as e:
        print(f"Une erreur majeure est survenue dans le bloc principal : {e}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("\nNavigateur fermé. Opération terminée.")