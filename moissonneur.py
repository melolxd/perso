import time
import os
import re
import random
import pandas as pd
from datetime import datetime
import undetected_chromedriver as uc
from bs4 import BeautifulSoup

# =============================================================================
# FONCTION 1 : Le Moissonneur (Inchangée)
# =============================================================================
def get_tournament_list_selenium(year, driver):
    print(f"--- PHASE 1: Recherche de tous les tournois pour l'année {year} ---")
    archive_url = f'https://www.atptour.com/en/scores/results-archive?year={year}'
    try:
        driver.get(archive_url)
        print("  -> Page des archives atteinte. Attente du chargement...")
        time.sleep(5)
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        tournaments = []
        tournament_links = soup.find_all('a', class_='tournament__profile')
        print(f"  -> {len(tournament_links)} liens de tournois potentiels trouvés.")
        for link in tournament_links:
            href = link.get('href', '')
            match = re.search(r'/tournaments/([^/]+)/(\d+)/', href)
            if match:
                tourney_name = match.group(1)
                tourney_id = match.group(2)
                tournaments.append({'name': tourney_name, 'id': tourney_id})
        unique_tournaments = [dict(t) for t in {tuple(d.items()) for d in tournaments}]
        print(f"  -> Extraction réussie de {len(unique_tournaments)} tournois uniques.")
        return unique_tournaments
    except Exception as e:
        print(f"  -> Erreur critique lors de la récupération de la liste des tournois : {e}")
        return []

# =============================================================================
# FONCTION 2 : Le Plongeur (La nouvelle fonction que vous avez demandée)
# =============================================================================
def get_detailed_stats(stats_url, driver):
    """
    Visite une page de statistiques de match et en extrait les détails.
    """
    print(f"      -> Plongée pour les stats : {stats_url.split('/')[-2]}")
    try:
        driver.get(stats_url)
        time.sleep(2.5) # Pause pour le chargement des stats
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        stats_dict = {}
        stats_container = soup.find('div', id='MatchStats')
        if not stats_container:
            return {}

        stat_sections = stats_container.find_all('div', class_='stat-section')
        for section in stat_sections:
            individual_stats = section.find_all('div', class_='statTileWrapper')
            for stat in individual_stats:
                label_element = stat.find('div', class_='label')
                if not label_element: continue
                stat_name = label_element.get_text(strip=True).lower().replace(' ', '_').replace('%','_pct')
                
                p1_stat_tag = stat.find('div', class_='p1Stats')
                p2_stat_tag = stat.find('div', class_='p2Stats')
                
                if p1_stat_tag and p2_stat_tag:
                    p1_value = p1_stat_tag.find('div', class_='labelBold').get_text(strip=True)
                    p2_value = p2_stat_tag.find('div', class_='labelBold').get_text(strip=True)
                    stats_dict[f'w_{stat_name}'] = p1_value
                    stats_dict[f'l_{stat_name}'] = p2_value
        return stats_dict
    except Exception as e:
        print(f"        -> Avertissement : Erreur lors du scraping des stats : {e}")
        return {}

# =============================================================================
# FONCTION 3 : L'Ouvrier (MODIFIÉE pour utiliser le plongeur)
# =============================================================================
def scrape_tournament_page(tourney_info, year, driver):
    """Utilise Selenium pour scraper les matchs et appeler le scraper de stats."""
    tourney_name = tourney_info['name'].replace('-', ' ').title()
    tourney_id = tourney_info['id']
    url = f"https://www.atptour.com/en/scores/archive/{tourney_info['name']}/{tourney_id}/{year}/results"
    
    print(f"\n  Scraping de : {tourney_name}")
    try:
        driver.get(url)
        time.sleep(4)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        parsed_matches = []
        match_blocks = soup.find_all('div', class_='match')

        for match in match_blocks:
            stats_items = match.find_all('div', class_='stats-item')
            if len(stats_items) < 2: continue
            
            winner_name_raw = stats_items[0].find('div', class_='player-info').get_text(strip=True)
            loser_name_raw = stats_items[1].find('div', class_='player-info').get_text(strip=True)
            if loser_name_raw.lower() == 'bye': continue

            # ... (logique de score) ...
            
            match_dict = {
                'tourney_name': tourney_name,
                'winner_name': winner_name_raw,
                'loser_name': loser_name_raw,
                'score': 'score_simulé' # Le score est aussi dans la page de stats, plus fiable
            }

            # --- INTÉGRATION DU PLONGEUR ---
            stats_link_tag = match.find('a', href=re.compile(r'/match-stats'))
            if stats_link_tag:
                stats_url = 'https://www.atptour.com' + stats_link_tag['href']
                detailed_stats = get_detailed_stats(stats_url, driver)
                match_dict.update(detailed_stats) # On ajoute les stats au dictionnaire
            
            parsed_matches.append(match_dict)

        print(f"    -> {len(parsed_matches)} matchs valides extraits.")
        return parsed_matches
    except Exception as e:
        print(f"  -- Erreur lors du traitement de ce tournoi : {e}")
        return []

# =============================================================================
# LE PROGRAMME PRINCIPAL QUI ORCHESTRE TOUT
# =============================================================================
if __name__ == "__main__":
    YEAR_TO_SCRAPE = 2025
    driver = None
    try:
        print("--- Initialisation du navigateur FURTIF ---")
        options = uc.ChromeOptions()
        driver = uc.Chrome(options=options, use_subprocess=True)
    
        tournaments_to_scrape = get_tournament_list_selenium(YEAR_TO_SCRAPE, driver)
    
        if tournaments_to_scrape:
            print("\n--- PHASE 2: Lancement du scraping détaillé ---")
            all_matches_of_the_year = []
            for i, tourney_info in enumerate(tournaments_to_scrape):
                print(f"\n--- Tournoi {i+1}/{len(tournaments_to_scrape)} ---")
                matches_from_tourney = scrape_tournament_page(tourney_info, YEAR_TO_SCRAPE, driver)
                if matches_from_tourney:
                    all_matches_of_the_year.extend(matches_from_tourney)
                time.sleep(random.uniform(2, 4))
            
            print("\n--- Scraping terminé ---")
            
            if all_matches_of_the_year:
                df_final = pd.DataFrame(all_matches_of_the_year)
                filename = f'atp_results_complet_avec_stats_{YEAR_TO_SCRAPE}.csv'
                df_final.to_csv(filename, index=False)
                
                print(f"\nSUCCÈS TOTAL ! {len(df_final)} matchs ont été sauvegardés dans {filename}")
                print("\nAperçu du fichier final avec les nouvelles colonnes de stats :")
                print(df_final.head())
            else:
                print("Échec : Aucun match n'a pu être scrapé et sauvegardé.")

    except Exception as e:
        print(f"Une erreur majeure est survenue : {e}")
    finally:
        if driver:
            driver.quit()
            print("\nNavigateur fermé. Opération terminée.")