from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os

# =============================================================================
# SCRIPT DE SCRAPING AVEC SELENIUM (Version finale)
# =============================================================================

# L'URL de la page des résultats du tournoi à scraper
url = 'https://www.atptour.com/en/scores/archive/los-cabos/7480/2024/results'

# Chemin vers votre chromedriver. Si il est dans le même dossier, c'est le plus simple.
# Assurez-vous que le nom du fichier est correct.
driver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
service = Service(executable_path=driver_path)
options = webdriver.ChromeOptions()
# Pour éviter que le navigateur ne s'ouvre de manière visible, vous pouvez dé-commenter la ligne suivante :
# options.add_argument('--headless') 

# Lancement du navigateur piloté
print("Lancement du navigateur piloté par Selenium...")
driver = None # Initialiser la variable driver
try:
    driver = webdriver.Chrome(service=service, options=options)
    print(f"Navigation vers l'URL : {url}")
    driver.get(url)

    # On attend 5 secondes. C'est crucial pour laisser le temps au JavaScript
    # (y compris les protections anti-bot) de se charger complètement.
    print("Attente du chargement complet de la page...")
    time.sleep(5) 

    print("Récupération du code HTML final de la page...")
    html_content = driver.page_source
    print("Code HTML récupéré avec succès.")

except Exception as e:
    print(f"Une erreur est survenue avec Selenium : {e}")
    # S'assurer que le driver est bien défini avant d'essayer de le fermer
    if driver:
        driver.quit()
    exit() # On arrête le script si le navigateur n'a pas pu charger la page
finally:
    # On s'assure de bien fermer le navigateur à la fin
    if driver:
        driver.quit()
        print("Navigateur fermé.")

# Le reste du script est identique à la version précédente, car on a maintenant le HTML propre
soup = BeautifulSoup(html_content, 'html.parser')

all_matches_data = []
match_blocks = soup.find_all('div', class_='match')
print(f"\nAnalyse du HTML : {len(match_blocks)} blocs de match trouvés.")

for match in match_blocks:
    try:
        stats_items = match.find_all('div', class_='stats-item')
        if len(stats_items) < 2:
            continue # Si le match n'a pas deux joueurs (ex: match de qualification bizarre), on l'ignore

        winner_block = stats_items[0]
        loser_block = stats_items[1]
        
        winner_name_div = winner_block.find('div', class_='player-info')
        loser_name_div = loser_block.find('div', class_='player-info')
        if not winner_name_div or not loser_name_div:
            continue

        winner_name = winner_name_div.get_text(strip=True)
        loser_name = loser_name_div.get_text(strip=True)
        
        winner_scores_divs = winner_block.find('div', class_='scores').find_all('div', class_='score-item')
        loser_scores_divs = loser_block.find('div', class_='scores').find_all('div', class_='score-item')
        
        set_scores = []
        for w_div, l_div in zip(winner_scores_divs, loser_scores_divs):
            w_score_span = w_div.find('span')
            l_score_span = l_div.find('span')
            if not w_score_span or not l_score_span:
                continue

            w_score = w_score_span.get_text(strip=True)
            l_score = l_score_span.get_text(strip=True)
            
            if w_score and l_score:
                w_tiebreak_span = w_div.find('span', class_='tie-break')
                l_tiebreak_span = l_div.find('span', class_='tie-break')

                if w_tiebreak_span:
                    set_scores.append(f"{w_score}({l_tiebreak_span.get_text(strip=True)})")
                elif l_tiebreak_span:
                    set_scores.append(f"{l_score}({w_tiebreak_span.get_text(strip=True)})")
                else:
                    set_scores.append(f"{w_score}-{l_score}")

        final_score = ' '.join(set_scores)

        match_dict = { 'winner_name': winner_name, 'loser_name': loser_name, 'score': final_score }
        all_matches_data.append(match_dict)

    except (IndexError, AttributeError) as e:
        print(f"Avertissement : Un bloc de match n'a pas pu être traité. Erreur: {e}")
        continue

if all_matches_data:
    df = pd.DataFrame(all_matches_data)
    today_str = datetime.now().strftime('%Y-%m-%d')
    filename = f'atp_selenium_scraped_{today_str}.csv'
    df.to_csv(filename, index=False)
    print(f"\nSuccès ! {len(df)} matchs ont été extraits et sauvegardés dans {filename}")
    print("\nAperçu des données:")
    print(df.head())
else:
    print("\nÉchec : Aucun match n'a pu être trouvé ou traité sur la page après analyse du HTML.")