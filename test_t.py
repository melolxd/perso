# -*- coding: utf-8 -*-
"""
ATP Scraper – Saison 2025  (v5 – 01-08-2025)
--------------------------------------------
• Découvre et scrape les 44 tournois ATP 2025 (tableau principal + qualifs)
• Récupère la feuille “Stats-Centre” quand dispo
• Gère automatiquement les challenges Cloudflare / hCaptcha
• Génère le CSV global  atp_matches_2025_ALL.csv
"""

###############################################################################
# Imports
###############################################################################
import re, time, random, traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, InvalidArgumentException

###############################################################################
# Paramètres
###############################################################################
YEAR            = 2025
OUTPUT_CSV      = f"atp_matches_{YEAR}_ALL.csv"
SKIP_QUALIFYING = False          # ← on garde les qualifs
MAX_MATCHES     = None           # ← None = pas de limite, scrape complet

###############################################################################
# Helpers généraux
###############################################################################
def parse_stat_value(text: str, idx: int = 0) -> int:
    nums = re.findall(r"\d+", text or "0")
    return int(nums[idx]) if len(nums) > idx else 0

def smart_wait(driver, tmin=2.0, tmax=5.0):
    time.sleep(random.uniform(tmin, tmax))
    if random.random() < 0.3:             # micro-mouvement souris
        try:
            ActionChains(driver).move_by_offset(random.randint(-10,10),
                                                random.randint(-10,10)).perform()
        except Exception:
            pass

###############################################################################
# Cloudflare / hCaptcha helper
###############################################################################

def safe_get(driver, url, max_attempts=3):
    """
    driver.get(url) + résolution du challenge Cloudflare.
    On NE teste plus le mot « cloudflare » présent dans les scripts d’analytics,
    seulement les marqueurs du challenge.
    """
    challenge_keys = [
        "just a moment",
        "checking your browser",
        "challenge-running",
        "cf-spinner-allow",
    ]

    for attempt in range(1, max_attempts + 1):
        driver.get(url)
        time.sleep(random.uniform(2, 4))

        # -------- boucle interne : la page est-elle encore en challenge ? ----
        for _ in range(5):                       # 5 sous-essais
            page = driver.page_source.lower()

            # Page OK ?
            if all(k not in page for k in challenge_keys):
                return True                      # plus de challenge → on sort

            print(f"        ↻ Cloudflare (try {attempt})…")
            try:
                # Chercher un iframe hCaptcha/reCAPTCHA
                iframe = driver.find_element(
                    By.CSS_SELECTOR,
                    ("iframe[src*='challenges.cloudflare.com'],"
                     "iframe[title*='Widget containing'],"
                     "iframe[data-hcaptcha-widget-id]")
                )
                driver.switch_to.frame(iframe)

                # Cliquer la checkbox
                checkbox = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "input[type='checkbox'], div[role='checkbox']"))
                )
                ActionChains(driver).move_to_element(checkbox).click().perform()
                print("        ☑️  Checkbox cliquée")
            except Exception:
                pass
            finally:
                driver.switch_to.default_content()

            time.sleep(random.uniform(6, 10))     # attendre la validation

        # Encore bloqué ? on ré-essaie avec un nouveau driver.get
        time.sleep(random.uniform(15, 25))

    print("        ❌ Cloudflare non résolu après plusieurs essais")
    return False

###############################################################################
# Driver furtif
###############################################################################
def setup_stealth_browser():
    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    try:
        opts.add_experimental_option("useAutomationExtension", False)
    except InvalidArgumentException:
        pass
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36")
    try:
        drv = uc.Chrome(options=opts, use_subprocess=True)
    except InvalidArgumentException:
        print("[WARN] options avancées refusées – relance basique …")
        drv = uc.Chrome()
    drv.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    drv.execute_script("Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3]})")
    drv.execute_script("Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']})")
    drv.execute_script("window.chrome={ runtime:{} }")
    return drv

###############################################################################
# Découverte des tournois
###############################################################################
def get_tournaments_list(year:int, driver):
    url = f"https://www.atptour.com/en/scores/results-archive?year={year}"
    if not safe_get(driver, url):
        raise RuntimeError("Archive page bloquée par Cloudflare")
    WebDriverWait(driver,20).until(
        EC.presence_of_all_elements_located((By.LINK_TEXT,"Results")))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    tourneys, seen = [], set()
    for a in soup.find_all("a", string="Results"):
        parts = a.get("href","").strip("/").split("/")
        if len(parts)>=6:
            key = (parts[3], parts[4])
            if key not in seen:
                seen.add(key)
                tourneys.append({"name": parts[3], "id": parts[4]})
    return sorted(tourneys, key=lambda d: d["name"])

###############################################################################
# Stats-Centre
###############################################################################
def get_detailed_stats(url, driver):
    """
    Renvoie (stats_dict, reason)
      • stats_dict : {} si vide / bloqué
      • reason     : '' (succès) | 'blocked' | 'empty'
    """
    if not safe_get(driver, url):
        return {}, "blocked"           # challenge non résolu

    try:
        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.presence_of_element_located((By.ID, "MatchStats")),
                EC.presence_of_element_located((By.CLASS_NAME, "statTileWrapper")))
        )
    except TimeoutException:
        return {}, "blocked"           # time-out → supposé bloqué

    soup = BeautifulSoup(driver.page_source, "html.parser")
    cont = soup.find("div", id="MatchStats")
    if not cont:
        return {}, "empty"             # page vide (ATP n’a rien publié)

    stats = {}
    for tile in cont.find_all("div", "statTileWrapper"):
        label = tile.find("div", "labelWrappper")
        if not label:
            continue
        p1 = tile.select_one(".p1Stats .non-speed")
        p2 = tile.select_one(".p2Stats .non-speed")
        if p1 and p2:
            stats[label.get_text(strip=True)] = (
                p1.get_text(strip=True),
                p2.get_text(strip=True),
            )
    return stats, ""                   # succès

###############################################################################
# Codes de ronde
###############################################################################
ROUND_MAP = {
    "Final":"F","Semifinals":"SF","Quarterfinals":"QF",
    "Round of 16":"R16","Round of 32":"R32","Round of 64":"R64","Round of 128":"R128",
    "First Round":"R32","Second Round":"R16","Third Round":"R8",
    "Qualifying – First Round":"Q1","Qualifying – Second Round":"Q2",
    "Qualifying – Third Round":"Q3","Qualifying – Final":"Q3",
}

###############################################################################
# Scraper un tournoi entier
###############################################################################
def scrape_tournament(t, year, driver):
    slug, tid = t["name"], t["id"]
    name = slug.replace("-", " ").title()
    url = f"https://www.atptour.com/en/scores/archive/{slug}/{tid}/{year}/results"
    print(f"\n→ {name} {year}")

    if not safe_get(driver, url):
        print("   bloqué par Cloudflare – skip")
        return []

    try:
        WebDriverWait(driver,20).until(
            EC.presence_of_element_located((By.CLASS_NAME,"atp_scores-results")))
    except TimeoutException:
        print("   page indisponible – skip")
        return []

    soup = BeautifulSoup(driver.page_source,"html.parser")

    # date & surface
    tourney_date, surface = "", "Hard"
    date_loc = soup.find("div","date-location")
    if date_loc:
        spans=[s.get_text(strip=True) for s in date_loc.find_all("span")]
        if len(spans)>=3:
            surface = spans[-1].split()[-1].title()
        raw = spans[0].split('–')[0].split('-')[0].strip()
        try:
            d,m = raw.split()
            tourney_date = datetime.strptime(f"{d} {m} {year}", "%d %b %Y").strftime("%Y%m%d")
        except ValueError:
            pass

    blocks = soup.select("div.atp_scores-results div.match")
    print(f"   {len(blocks)} matches trouvés")
    match_num = len(blocks)+1
    data=[]

    for blk in blocks:
        smart_wait(driver,1.5,3.5)
        match_num-=1

        header_raw = blk.find("div","match-header").find("span").get_text(strip=True)
        header_clean = re.split(r'\s*[-–—]\s*', header_raw)[0].rstrip('.').strip()
        if SKIP_QUALIFYING and "qualifying" in header_clean.lower():
            continue
        rnd = ROUND_MAP.get(header_clean, "Unknown")

        # durée
        try:
            hms = blk.select_one(".match-header span:last-of-type").get_text(strip=True)
            h,m,_ = map(int,hms.split(":")); minutes = h*60+m
        except Exception: minutes = 0

        s_items = blk.find_all("div","stats-item")
        if len(s_items)<2: continue

        def player(tag):
            info = tag.find("div","player-info")
            name = info.find("div","name").find("a").get_text(strip=True)
            pid  = info.find("a")["href"].split("/")[3]
            ioc  = info.find("svg","atp-flag").find("use")["href"].split("-")[-1].upper()
            extra= (info.find("div","name").find("span") or BeautifulSoup("","html.parser")).get_text(strip=True).strip("()")
            seed, entry = ("","")
            if extra.upper() in {"WC","Q","LL"}: entry=extra.upper()
            else: seed=extra
            return name,pid,ioc,seed,entry

        w_n,w_id,w_ioc,w_seed,w_entry = player(s_items[0])
        l_n,l_id,l_ioc,l_seed,l_entry = player(s_items[1])

        score_txt = (blk.find("div","match-notes") or BeautifulSoup("","html.parser")).get_text(strip=True)\
                    .replace("Game Set and Match","")
        score = score_txt.split("wins the match")[-1].strip() if "wins the match" in score_txt else score_txt or "N/A"

        print(f"      [{match_num:02}] {w_n} def. {l_n} ({rnd}) … ", end="")

        match = {
            "tourney_id":f"{year}-{tid}","tourney_name":name,"surface":surface,"draw_size":32,
            "tourney_level":"A","tourney_date":tourney_date,"match_num":match_num,
            "winner_id":w_id,"winner_seed":w_seed,"winner_entry":w_entry,"winner_name":w_n,
            "winner_hand":"R","winner_ht":0,"winner_ioc":w_ioc,"winner_age":0.0,
            "loser_id":l_id,"loser_seed":l_seed,"loser_entry":l_entry,"loser_name":l_n,
            "loser_hand":"R","loser_ht":0,"loser_ioc":l_ioc,"loser_age":0.0,
            "score":score,"best_of":3,"round":rnd,"minutes":minutes,
        }

                # ── Stats-Centre ───────────────────────────────────────────────
        tag = blk.find("a", href=re.compile(r"/en/scores/stats-centre/archive/"))
        if tag:
            stats, why = get_detailed_stats("https://www.atptour.com" + tag["href"], driver)
        else:
            stats, why = {}, "no_link"

        # Journalisation détaillée
        if   why == "":        print("stats ✔")
        elif why == "empty":   print("stats ✘  (page vide – ATP n’a rien publié)")
        elif why == "blocked": print("stats ✘  (bloqué Cloudflare)")
        elif why == "no_link": print("stats ✘  (aucun lien Stats-Centre)")
        else:                  print("stats ✘  (?)")


        def single(k, lab):
            w,l = stats.get(lab,("0","0"))
            match[f"w_{k}"]=parse_stat_value(w)
            match[f"l_{k}"]=parse_stat_value(l)
        def ratio(pref, lab):
            w,l = stats.get(lab,("0/0","0/0"))
            match[f"w_{pref}In"]=parse_stat_value(w,0)
            match[f"w_{pref}Tot"]=parse_stat_value(w,1)
            match[f"l_{pref}In"]=parse_stat_value(l,0)
            match[f"l_{pref}Tot"]=parse_stat_value(l,1)

        single("ace","Aces"); single("df","Double Faults")
        ratio("sv","First serve")
        single("1stWon","1st serve points won"); single("2ndWon","2nd serve points won")
        ratio("bp","Break Points Saved"); single("SvGms","Service Games Played")

        for p in ("winner","loser"):
            match[f"{p}_rank"]=0; match[f"{p}_rank_points"]=0
        data.append(match)

        # arrêt test éventuel
        if MAX_MATCHES and len(data)>=MAX_MATCHES:
            break

    print(f"   {len(data)} matches valides")
    return data

###############################################################################
# Main
###############################################################################
if __name__=="__main__":
    header=[
        "tourney_id","tourney_name","surface","draw_size","tourney_level","tourney_date","match_num",
        "winner_id","winner_seed","winner_entry","winner_name","winner_hand","winner_ht","winner_ioc","winner_age",
        "loser_id","loser_seed","loser_entry","loser_name","loser_hand","loser_ht","loser_ioc","loser_age",
        "score","best_of","round","minutes",
        "w_ace","w_df","w_svIn","w_svTot","w_1stWon","w_2ndWon","w_SvGms","w_bpIn","w_bpTot",
        "l_ace","l_df","l_svIn","l_svTot","l_1stWon","l_2ndWon","l_SvGms","l_bpIn","l_bpTot",
        "winner_rank","winner_rank_points","loser_rank","loser_rank_points",
    ]
    int_cols=[c for c in header if c.startswith(("w_","l_","minutes"))]

    driver=None
    try:
        print("=== Initialisation navigateur furtif ===")
        driver = setup_stealth_browser()

        tourneys = get_tournaments_list(YEAR, driver)
        print(f"{len(tourneys)} tournois détectés pour {YEAR}")

        all_matches=[]
        for t in tourneys:
            try:
                all_matches.extend(scrape_tournament(t, YEAR, driver))
                if MAX_MATCHES and len(all_matches)>=MAX_MATCHES:
                    print(f"\n[TEST] limite {MAX_MATCHES} matches → sortie")
                    break
                smart_wait(driver,4,8)
            except Exception:
                print(f"[WARN] tournoi {t['name']} échoué"); traceback.print_exc()

        if not all_matches:
            raise RuntimeError("Aucun match collecté")

        df = pd.DataFrame(all_matches, columns=header).fillna(0)
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)
        df.to_csv(Path(__file__).with_name(OUTPUT_CSV), index=False, encoding="utf-8-sig")
        print(f"\n✅ CSV global créé : {OUTPUT_CSV}  ({len(df)} lignes)")

    except Exception as e:
        print(f"[ERREUR] {e}"); traceback.print_exc()
    finally:
        if driver:
            driver.quit(); print("\nNavigateur fermé – fin.")
