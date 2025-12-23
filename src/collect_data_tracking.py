"""
Collecteur de données avec TRACKING HISTORIQUE
Évite la collecte de doublons entre les exécutions
VERSION CORRIGÉE - Chemins adaptés à votre structure
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
import feedparser
from bs4 import BeautifulSoup
import requests
import tweepy
from datetime import datetime, timedelta, timezone
import praw
from dotenv import load_dotenv
import time
import hashlib
from urllib.parse import urljoin
import logging
from typing import List, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from dotenv import load_dotenv, find_dotenv


# --------------------------
# CONFIGURATION UTF-8 POUR WINDOWS
# --------------------------
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

path = find_dotenv()
print("🌍 .env trouvé :", path if path else "❌ Aucun fichier trouvé")

# 📥 Charger les variables d’environnement
load_dotenv(path)

# 🧩 Vérifier que la clé est bien chargée
print("🔑 NEWS_API_KEY =", os.getenv("NEWS_API_KEY"))
load_dotenv()

# --------------------------
# Configuration des chemins (CORRIGÉ)
# --------------------------
# Obtenir le dossier du script (src/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Remonter au dossier parent (News_Trend_Analysis/)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Chemins absolus vers les dossiers de données
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
TRACKING_DIR = os.path.join(PROJECT_ROOT, "data", "tracking")

# Créer les sous-dossiers
for subdir in ['rss', 'twitter', 'reddit', 'scraping', 'newsapi', 'combined']:
    os.makedirs(os.path.join(RAW_DIR, subdir), exist_ok=True)
os.makedirs(TRACKING_DIR, exist_ok=True)

logger.info(f"[Config] Dossier projet: {PROJECT_ROOT}")
logger.info(f"[Config] Données brutes: {RAW_DIR}")
logger.info(f"[Config] Tracking: {TRACKING_DIR}")

# --------------------------
# 🔑 SYSTÈME DE TRACKING HISTORIQUE
# --------------------------
class DataTracker:
    """Gère le suivi des données déjà collectées"""
    
    def __init__(self):
        self.tracking_file = os.path.join(TRACKING_DIR, "collected_hashes.json")
        self.history_file = os.path.join(TRACKING_DIR, "collection_history.json")
        self.known_hashes: Set[str] = set()
        self.load_tracking()
    
    def load_tracking(self):
        """Charge l'historique des hashes collectés"""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.known_hashes = set(data.get('hashes', []))
                logger.info(f"[Tracking] {len(self.known_hashes):,} hashes charges depuis l'historique")
            except Exception as e:
                logger.error(f"[Tracking] Erreur chargement: {e}")
                self.known_hashes = set()
        else:
            logger.info("[Tracking] Aucun historique trouve, creation d'un nouveau fichier")
    
    def is_new(self, content_hash: str) -> bool:
        """Vérifie si le hash est nouveau"""
        return content_hash not in self.known_hashes
    
    def add_hash(self, content_hash: str):
        """Ajoute un hash à la liste des connus"""
        self.known_hashes.add(content_hash)
    
    def save_tracking(self):
        """Sauvegarde l'historique mis à jour"""
        try:
            with open(self.tracking_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'hashes': list(self.known_hashes),
                    'last_updated': datetime.now().isoformat(),
                    'total_count': len(self.known_hashes)
                }, f, indent=2)
            logger.info(f"[Tracking] {len(self.known_hashes):,} hashes sauvegardes")
        except Exception as e:
            logger.error(f"[Tracking] Erreur sauvegarde: {e}")
    
    def save_collection_stats(self, stats: Dict):
        """Enregistre les statistiques de la collecte"""
        history = []
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except:
                pass
        
        history.append({
            'timestamp': datetime.now().isoformat(),
            'stats': stats
        })
        
        # Garder seulement les 100 dernières collectes
        history = history[-100:]
        
        with open(self.history_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2)

# Instance globale du tracker
tracker = DataTracker()

# --------------------------
# Utilitaires
# --------------------------
def generate_hash(text: str) -> str:
    """Génère un hash unique pour détecter les doublons"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def classify_news_type(title: str, summary: str = "", category: str = "") -> str:
    """Classifie automatiquement le type de news basé sur le contenu"""
    
    # Combiner titre + résumé pour analyse
    text = (title + " " + summary).lower()
    
    # Si catégorie déjà fournie (NewsAPI), l'utiliser
    if category:
        return category
    
    # Mots-clés par catégorie
    tech_keywords = ['ai', 'intelligence artificielle', 'tech', 'technolog', 'software', 
                     'app', 'digital', 'cyber', 'robot', 'internet', 'data', 'algorithm',
                     'startup', 'google', 'apple', 'microsoft', 'meta', 'tesla']
    
    business_keywords = ['business', 'market', 'stock', 'économie', 'economy', 'finance',
                        'bank', 'trade', 'invest', 'company', 'ceo', 'entreprise',
                        'dollar', 'euro', 'bourse', 'croissance']
    
    politics_keywords = ['politic', 'government', 'election', 'president', 'minister',
                        'parlement', 'vote', 'law', 'congress', 'senate', 'diplomacy',
                        'gouvernement', 'ministre', 'député']
    
    science_keywords = ['science', 'research', 'study', 'scientist', 'discover',
                       'space', 'nasa', 'climat', 'climate', 'environment', 'energy',
                       'médical', 'health', 'covid', 'vaccine', 'cancer']
    
    sports_keywords = ['sport', 'football', 'basketball', 'tennis', 'match', 'game',
                      'player', 'team', 'champion', 'olympic', 'world cup', 'league']
    
    entertainment_keywords = ['film', 'movie', 'music', 'actor', 'celebrity', 'hollywood',
                             'series', 'tv', 'concert', 'album', 'netflix', 'disney']
    
    # Compter les occurrences
    scores = {
        'technology': sum(1 for kw in tech_keywords if kw in text),
        'business': sum(1 for kw in business_keywords if kw in text),
        'politics': sum(1 for kw in politics_keywords if kw in text),
        'science': sum(1 for kw in science_keywords if kw in text),
        'sports': sum(1 for kw in sports_keywords if kw in text),
        'entertainment': sum(1 for kw in entertainment_keywords if kw in text)
    }
    
    # Retourner la catégorie avec le score le plus élevé
    max_category = max(scores, key=scores.get)
    
    # Si aucun match significatif, retourner 'general'
    if scores[max_category] == 0:
        return 'general'
    
    return max_category

def save_data(data: List[Dict], filename: str, format='both'):
    """Sauvegarde les données en CSV et JSON"""
    if not data:
        logger.warning(f"Aucune donnee a sauvegarder pour {filename}")
        return
    
    df = pd.DataFrame(data)
    
    if format in ['csv', 'both']:
        df.to_csv(f"{filename}.csv", index=False, encoding='utf-8')
        logger.info(f"[OK] Sauvegarde: {filename}.csv ({len(data)} entrees)")
    
    if format in ['json', 'both']:
        df.to_json(f"{filename}.json", orient="records", force_ascii=False, indent=2)
        logger.info(f"[OK] Sauvegarde: {filename}.json ({len(data)} entrees)")

# --------------------------
# 1- RSS avec filtrage temporel
# --------------------------
def parse_single_feed(source: str, url: str, hours_back: int = 24) -> List[Dict]:
    """Parse un seul feed RSS avec filtrage temporel"""
    articles = []
    cutoff_time = datetime.now() - timedelta(hours=hours_back)
    
    try:
        feed = feedparser.parse(url)
        
        for entry in feed.entries:
            # Extraction du contenu
            summary = ""
            if hasattr(entry, 'summary'):
                summary = BeautifulSoup(entry.summary, "html.parser").get_text()
            elif hasattr(entry, 'description'):
                summary = BeautifulSoup(entry.description, "html.parser").get_text()
            elif hasattr(entry, 'content'):
                summary = BeautifulSoup(str(entry.content), "html.parser").get_text()
            
            # Extraction de la date
            published = ""
            entry_date = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                entry_date = datetime(*entry.published_parsed[:6])
                published = entry_date.isoformat()
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                entry_date = datetime(*entry.updated_parsed[:6])
                published = entry_date.isoformat()
            
            # FILTRE TEMPOREL: Ignorer les articles trop anciens
            if entry_date and entry_date < cutoff_time:
                continue
            
            title = entry.title if hasattr(entry, 'title') else ""
            link = entry.link if hasattr(entry, 'link') else ""
            
            content_hash = generate_hash(title + link)
            
            # VÉRIFICATION: Ignorer si déjà collecté
            if not tracker.is_new(content_hash):
                continue
            
            article = {
                "source_type": "rss",
                "source": source,
                "title": title,
                "link": link,
                "published": published,
                "summary": summary[:500],
                "news_type": classify_news_type(title, summary[:200]),  # NOUVEAU
                "content_hash": content_hash,
                "retrieved_date": datetime.now().isoformat()
            }
            articles.append(article)
            tracker.add_hash(content_hash)
        
        logger.info(f"  [OK] {source}: {len(articles)} nouveaux articles")
        return articles
        
    except Exception as e:
        logger.error(f"  [ERREUR] {source}: {e}")
        return []

def collect_rss(hours_back: int = 24):
    """Collecte RSS avec filtrage temporel"""
    
    # Chercher feeds.json dans src/ (CORRIGÉ)
    feeds_path = os.path.join(SCRIPT_DIR, "feeds.json")
    
    if not os.path.exists(feeds_path):
        logger.error(f"[RSS] Fichier feeds.json introuvable!")
        logger.error(f"       Cherche dans: {feeds_path}")
        return []
    
    try:
        with open(feeds_path, "r", encoding='utf-8') as f:
            rss_feeds = json.load(f)
        logger.info(f"[RSS] {len(rss_feeds)} feeds charges depuis {feeds_path}")
    except Exception as e:
        logger.error(f"[RSS] Erreur lecture feeds.json: {e}")
        return []
    
    logger.info(f"[RSS] Collecte des articles des {hours_back} dernieres heures")
    
    all_articles = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(parse_single_feed, source, url, hours_back): source 
                   for source, url in rss_feeds.items()}
        
        for future in as_completed(futures):
            articles = future.result()
            all_articles.extend(articles)
    
    today = datetime.today().strftime("%Y-%m-%d_%H%M")
    save_data(all_articles, os.path.join(RAW_DIR, "rss", f"articles_{today}"))
    logger.info(f"[RSS] Total: {len(all_articles)} nouveaux articles")
    return all_articles

# --------------------------
# 2- NewsAPI avec filtrage
# --------------------------

def collect_newsapi(hours_back: int = 24):
    """Collecte NewsAPI avec filtrage temporel et gestion UTC propre."""

    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        logger.warning("[NewsAPI] Clé API manquante dans .env")
        return []

    articles = []
    countries = ['us', 'fr']
    categories = ['business', 'entertainment', 'health', 'science', 'sports', 'technology']

    logger.info(f"[NewsAPI] Collecte des articles des {hours_back} dernières heures")
    
    # ✅ Fuseau UTC correct et sans avertissement
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    request_count = 0
    max_requests = 4

    for country in countries:
        for category in categories:
            if request_count >= max_requests:
                break
            request_count += 1

            logger.info(f"[NewsAPI] ({request_count}/{max_requests}) {country}/{category}")

            url = "https://newsapi.org/v2/top-headlines"
            params = {
                'apiKey': api_key,
                'country': country,
                'category': category,
                'pageSize': 100
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get('status') == 'ok':
                    total = len(data.get("articles", []))
                    kept = 0

                    for art in data.get("articles", []):
                        pub_date = art.get("publishedAt")

                        # Vérification de la date
                        if pub_date:
                            try:
                                article_date = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                                # Comparaison UTC → UTC
                                if article_date < cutoff_time:
                                    continue
                            except Exception as e:
                                logger.debug(f"  [Erreur parsing date] {pub_date}: {e}")

                        # Génération du hash unique
                        content_hash = generate_hash((art.get("title") or "") + (art.get("url") or ""))

                        # Vérification de nouveauté
                        if not tracker.is_new(content_hash):
                            continue

                        # Ajout de l'article
                        articles.append({
                            "source_type": "newsapi",
                            "source": art.get("source", {}).get("name", "Unknown"),
                            "country": country,
                            "category": category,
                            "title": art.get("title"),
                            "description": art.get("description"),
                            "url": art.get("url"),
                            "publishedAt": pub_date,
                            "content": art.get("content"),
                            "news_type": category,
                            "content_hash": content_hash,
                            "retrieved_date": datetime.now(timezone.utc).isoformat()
                        })
                        tracker.add_hash(content_hash)
                        kept += 1

                    logger.info(f"  [OK] {total} articles (garde {kept})")
                else:
                    logger.error(f"  [ERREUR] {data.get('message', 'Erreur inconnue')}")

                time.sleep(1)

            except Exception as e:
                logger.error(f"  [ERREUR]: {e}")

    # Sauvegarde des articles collectés
    today = datetime.now().strftime("%Y-%m-%d_%H%M")
    save_data(articles, os.path.join(RAW_DIR, "newsapi", f"articles_{today}"))
    logger.info(f"[NewsAPI] Total: {len(articles)} nouveaux articles")

    return articles
# --------------------------
# 3- Twitter DESACTIVE
# --------------------------
def collect_twitter(hours_back: int = 24):
    """Twitter désactivé - 176 feeds RSS suffisent"""
    logger.info("[Twitter] DESACTIVE - API payante depuis 2023")
    logger.info("[Twitter] Les 176 feeds RSS fournissent plus de donnees")
    logger.info("[Twitter] Pour activer: configurer TWITTER_BEARER_TOKEN dans .env et modifier cette fonction")
    return []
# --------------------------
# 4- Reddit avec filtrage
# --------------------------
def collect_reddit(hours_back: int = 24):
    """Collecte Reddit avec filtrage temporel"""
    
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "NewsCollector/1.0")
    
    if not (client_id and client_secret) or client_id == "votre_client_id_ici":
        logger.warning("[Reddit] Identifiants manquants")
        return []
    
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        logger.info("[Reddit] Connexion etablie")
    except Exception as e:
        logger.error(f"[Reddit] Erreur connexion: {e}")
        return []
    
    subreddits = ["worldnews", "news", "technology", "science", "business"]
    posts_data = []
    cutoff_time = datetime.now() - timedelta(hours=hours_back)
    
    logger.info(f"[Reddit] Collecte des posts des {hours_back} dernieres heures")
    
    for subreddit_name in subreddits:
        logger.info(f"[Reddit] r/{subreddit_name}")
        
        try:
            subreddit = reddit.subreddit(subreddit_name)
            
            for post in subreddit.new(limit=100):
                post_date = datetime.fromtimestamp(post.created_utc)
                if post_date < cutoff_time:
                    continue
                
                content_hash = generate_hash(post.title + post.url)
                
                if not tracker.is_new(content_hash):
                    continue
                
                posts_data.append({
                    "source_type": "reddit",
                    "subreddit": subreddit_name,
                    "title": post.title,
                    "url": post.url,
                    "score": post.score,
                    "created_utc": post_date.isoformat(),
                    "news_type": classify_news_type(post.title),  # NOUVEAU
                    "content_hash": content_hash,
                    "retrieved_date": datetime.now().isoformat()
                })
                tracker.add_hash(content_hash)
            
            logger.info(f"  [OK] {len([p for p in posts_data if p['subreddit'] == subreddit_name])} nouveaux posts")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"  [ERREUR]: {e}")
    
    today = datetime.today().strftime("%Y-%m-%d_%H%M")
    save_data(posts_data, os.path.join(RAW_DIR, "reddit", f"posts_{today}"))
    logger.info(f"[Reddit] Total: {len(posts_data)} nouveaux posts")
    return posts_data

# --------------------------
# 5- Scraping avec filtrage
# --------------------------
def collect_scraping():
    """Scraping avec filtrage"""
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    sites = {
    "BBC": "https://www.bbc.com/news",
    "Reuters": "https://www.reuters.com/world/",
    "Guardian": "https://www.theguardian.com/international",
    "CNN": "https://edition.cnn.com/world",
    "NYTimes": "https://www.nytimes.com/section/world",
    "WashingtonPost": "https://www.washingtonpost.com/world/",
    "AlJazeera": "https://www.aljazeera.com/news/",
    "France24": "https://www.france24.com/en/",
    "LeMonde": "https://www.lemonde.fr/",
    "LeFigaro": "https://www.lefigaro.fr/",
    "ElPais": "https://elpais.com/",
    "DeutscheWelle": "https://www.dw.com/en/top-stories/s-9097",
    "DerSpiegel": "https://www.spiegel.de/international/",
    "CBC": "https://www.cbc.ca/news",
    "AP": "https://apnews.com/hub/world-news",
    "Bloomberg": "https://www.bloomberg.com/europe",
    "Politico": "https://www.politico.eu/",
    "TheEconomist": "https://www.economist.com/",
    "TheVerge": "https://www.theverge.com/",
    "TechCrunch": "https://techcrunch.com/",
    "Wired": "https://www.wired.com/",
    "HindustanTimes": "https://www.hindustantimes.com/world-news",
    "TheHindu": "https://www.thehindu.com/news/",
    "JapanTimes": "https://www.japantimes.co.jp/news/",
    "ArabNews": "https://www.arabnews.com/",
    "AlAhram": "https://english.ahram.org.eg/",
    "TimesOfIsrael": "https://www.timesofisrael.com/",
    "CBC (Canada)": "https://www.cbc.ca/news",
    "ABC_Australia": "https://www.abc.net.au/news/",
    }
    
    all_articles = []
    
    for source, url in sites.items():
        logger.info(f"[Scraping] {source}")
        
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            
            for tag in soup.find_all(['h1', 'h2', 'h3'], limit=100):
                text = tag.get_text(strip=True)
                
                if len(text) < 15 or len(text) > 300:
                    continue
                
                link = ""
                link_tag = tag.find("a") or tag.find_parent("a")
                if link_tag and link_tag.get("href"):
                    href = link_tag["href"]
                    link = urljoin(url, href) if not href.startswith('http') else href
                
                content_hash = generate_hash(text + link)
                
                if not tracker.is_new(content_hash):
                    continue
                
                all_articles.append({
                    "source_type": "scraping",
                    "source": source,
                    "title": text,
                    "link": link,
                    "news_type": classify_news_type(text),  # NOUVEAU
                    "content_hash": content_hash,
                    "retrieved_date": datetime.now().isoformat()
                })
                tracker.add_hash(content_hash)
            
            logger.info(f"  [OK] {len([a for a in all_articles if a['source'] == source])} nouveaux articles")
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"  [ERREUR]: {e}")
    
    today = datetime.today().strftime("%Y-%m-%d_%H%M")
    save_data(all_articles, os.path.join(RAW_DIR, "scraping", f"scraped_articles_{today}"))
    logger.info(f"[Scraping] Total: {len(all_articles)} nouveaux articles")
    return all_articles

# --------------------------
# Fusion
# --------------------------
def combine_all_sources():
    """Combine toutes les sources"""
    
    today = datetime.today().strftime("%Y-%m-%d")
    all_data = []
    
    for subdir in ['rss', 'twitter', 'reddit', 'scraping', 'newsapi']:
        subdir_path = os.path.join(RAW_DIR, subdir)
        if not os.path.exists(subdir_path):
            continue
            
        json_files = [f for f in os.listdir(subdir_path) 
                      if f.endswith('.json') and today in f]
        
        for file in sorted(json_files, reverse=True)[:1]:
            try:
                with open(os.path.join(subdir_path, file), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.extend(data)
            except Exception as e:
                logger.error(f"Erreur lecture {file}: {e}")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    save_data(all_data, os.path.join(RAW_DIR, "combined", f"all_sources_{timestamp}"))
    
    logger.info(f"[Fusion] Total: {len(all_data)} articles combines")
    return all_data

# --------------------------
# Exécution principale
# --------------------------
if __name__ == "__main__":
    
    print("\n" + "="*80)
    print("COLLECTEUR AVEC TRACKING HISTORIQUE")
    print("="*80 + "\n")
    
    HOURS_BACK = 24
    
    start_time = datetime.now()
    stats = {"new": {}, "skipped": 0}
    
    initial_hash_count = len(tracker.known_hashes)
    
    print(f"Hashes connus: {initial_hash_count:,}")
    print(f"Periode de collecte: {HOURS_BACK} dernieres heures\n")
    
    # Collecte
    print("\nPhase 1/5: RSS")
    print("-" * 80)
    rss_data = collect_rss(hours_back=HOURS_BACK)
    stats["new"]["rss"] = len(rss_data)
    
    print("\nPhase 2/5: NewsAPI")
    print("-" * 80)
    newsapi_data = collect_newsapi(hours_back=HOURS_BACK)
    stats["new"]["newsapi"] = len(newsapi_data)
    
    print("\nPhase 3/5: Twitter")
    print("-" * 80)
    twitter_data = collect_twitter(hours_back=HOURS_BACK)
    stats["new"]["twitter"] = len(twitter_data)
    
    print("\nPhase 4/5: Reddit")
    print("-" * 80)
    reddit_data = collect_reddit(hours_back=HOURS_BACK)
    stats["new"]["reddit"] = len(reddit_data)
    
    print("\nPhase 5/5: Scraping")
    print("-" * 80)
    scraping_data = collect_scraping()
    stats["new"]["scraping"] = len(scraping_data)
    
    print("\nFusion finale")
    print("-" * 80)
    combined_data = combine_all_sources()
    
    tracker.save_tracking()
    
    final_hash_count = len(tracker.known_hashes)
    new_hashes_added = final_hash_count - initial_hash_count
    
    stats["skipped"] = sum(stats["new"].values()) - new_hashes_added if new_hashes_added < sum(stats["new"].values()) else 0
    
    duration = (datetime.now() - start_time).total_seconds()
    total_new = sum(stats["new"].values())
    
    print("\n" + "="*80)
    print("COLLECTE TERMINEE")
    print("="*80)
    print(f"Duree: {duration:.1f}s ({duration/60:.1f} min)")
    print(f"Nouveaux: {total_new:,} elements")
    print(f"Total hashes: {final_hash_count:,} (+{new_hashes_added:,})")
    print(f"Doublons evites: {stats['skipped']:,}\n")
    
    print("Details par source:")
    print("-" * 80)
    for source, count in sorted(stats["new"].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_new * 100) if total_new > 0 else 0
        print(f"   {source.upper():15s} : {count:6,} ({percentage:5.1f}%)")
    
    print("\n" + "="*80)
    print("Donnees pretes!")
    print(f"Raw: {RAW_DIR}")
    print(f"Tracking: {TRACKING_DIR}")
    print("="*80 + "\n")
    
    tracker.save_collection_stats({
        'total_new': total_new,
        'by_source': stats["new"],
        'duration_seconds': duration,
        'hours_back': HOURS_BACK,
        'total_hashes': final_hash_count
    })