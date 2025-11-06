"""
Collecteur de données optimisé - Version améliorée de votre code
Maximise le volume de données avec vos feeds.json existants
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
import feedparser
from bs4 import BeautifulSoup
import requests
import tweepy
import praw
from dotenv import load_dotenv
import time
import hashlib
from urllib.parse import urljoin
import logging
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Charger les variables d'environnement
load_dotenv()

# --------------------------
# Configuration des dossiers
# --------------------------
RAW_DIR = "data/raw"
for subdir in ['rss', 'twitter', 'reddit', 'scraping', 'newsapi', 'combined']:
    os.makedirs(f"{RAW_DIR}/{subdir}", exist_ok=True)

# --------------------------
# Utilitaires
# --------------------------
def generate_hash(text: str) -> str:
    """Génère un hash unique pour détecter les doublons"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def save_data(data: List[Dict], filename: str, format='both'):
    """Sauvegarde les données en CSV et JSON"""
    if not data:
        logger.warning(f"Aucune donnée à sauvegarder pour {filename}")
        return
    
    df = pd.DataFrame(data)
    
    if format in ['csv', 'both']:
        df.to_csv(f"{filename}.csv", index=False, encoding='utf-8')
        logger.info(f"✓ Sauvegardé: {filename}.csv ({len(data)} entrées)")
    
    if format in ['json', 'both']:
        df.to_json(f"{filename}.json", orient="records", force_ascii=False, indent=2)
        logger.info(f"✓ Sauvegardé: {filename}.json ({len(data)} entrées)")

# --------------------------
# 1- RSS - OPTIMISÉ avec votre feeds.json
# --------------------------
def parse_single_feed(source: str, url: str) -> List[Dict]:
    """Parse un seul feed RSS (pour parallélisation)"""
    articles = []
    try:
        feed = feedparser.parse(url)
        
        for entry in feed.entries:
            # Extraction robuste du contenu
            summary = ""
            if hasattr(entry, 'summary'):
                summary = BeautifulSoup(entry.summary, "html.parser").get_text()
            elif hasattr(entry, 'description'):
                summary = BeautifulSoup(entry.description, "html.parser").get_text()
            elif hasattr(entry, 'content'):
                summary = BeautifulSoup(str(entry.content), "html.parser").get_text()
            
            # Extraction de la date
            published = ""
            if hasattr(entry, 'published'):
                published = entry.published
            elif hasattr(entry, 'updated'):
                published = entry.updated
            elif hasattr(entry, 'pubDate'):
                published = entry.pubDate
            
            # Extraction du titre
            title = entry.title if hasattr(entry, 'title') else ""
            link = entry.link if hasattr(entry, 'link') else ""
            
            article = {
                "source_type": "rss",
                "source": source,
                "title": title,
                "link": link,
                "published": published,
                "summary": summary[:500],  # Limiter la taille
                "content_hash": generate_hash(title + link),
                "retrieved_date": datetime.now().isoformat()
            }
            articles.append(article)
        
        logger.info(f"  ✓ {source}: {len(articles)} articles")
        return articles
        
    except Exception as e:
        logger.error(f"  ✗ {source}: {e}")
        return []

def collect_rss():
    """Collecte RSS avec votre fichier feeds.json - VERSION PARALLÈLE"""
    
    # Charger feeds.json
    try:
        with open("feeds.json", "r", encoding='utf-8') as f:
            rss_feeds = json.load(f)
        logger.info(f"[RSS] {len(rss_feeds)} feeds chargés depuis feeds.json")
    except FileNotFoundError:
        logger.error("[RSS] ⚠️ Fichier feeds.json introuvable!")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"[RSS] ⚠️ Erreur lecture feeds.json: {e}")
        return []
    
    all_articles = []
    
    # Traitement parallèle pour aller plus vite
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(parse_single_feed, source, url): source 
                   for source, url in rss_feeds.items()}
        
        for future in as_completed(futures):
            articles = future.result()
            all_articles.extend(articles)
    
    today = datetime.today().strftime("%Y-%m-%d")
    save_data(all_articles, f"{RAW_DIR}/rss/articles_{today}")
    logger.info(f"[RSS] ✅ Total: {len(all_articles)} articles")
    return all_articles

# --------------------------
# 2- NewsAPI - OPTIMISÉ (stratégie multi-pays)
# --------------------------
def collect_newsapi():
    """Collecte NewsAPI avec stratégie HEADLINES (meilleure pour version gratuite)"""
    
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key or api_key == "votre_cle_newsapi_ici":
        logger.warning("[NewsAPI] ⚠️ Clé API manquante ou invalide")
        return []
    
    articles = []
    
    # STRATÉGIE OPTIMALE: Top headlines par pays/catégorie
    # Version gratuite: accès complet aux headlines
    countries = ['us', 'gb', 'fr', 'de', 'ca', 'au', 'it', 'es', 'jp', 'kr']
    categories = ['general', 'business', 'technology', 'science', 'health', 'entertainment', 'sports']
    
    total_requests = len(countries) * len(categories)
    logger.info(f"[NewsAPI] {total_requests} requêtes prévues (limite: 100/jour)")
    
    request_count = 0
    
    for country in countries:
        for category in categories:
            request_count += 1
            
            # Arrêter si on approche de la limite (garder une marge)
            if request_count >= 95:
                logger.warning("[NewsAPI] ⚠️ Limite de requêtes approchée, arrêt")
                break
            
            logger.info(f"[NewsAPI] ({request_count}/{total_requests}) {country}/{category}")
            
            url = "https://newsapi.org/v2/top-headlines"
            params = {
                'apiKey': api_key,
                'country': country,
                'category': category,
                'pageSize': 100  # Maximum
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 426:
                    logger.error("[NewsAPI] ⚠️ Upgrade requis (plan gratuit limité)")
                    break
                
                response.raise_for_status()
                data = response.json()
                
                if data['status'] == 'ok':
                    for art in data.get("articles", []):
                        articles.append({
                            "source_type": "newsapi",
                            "source": art.get("source", {}).get("name", "Unknown"),
                            "country": country,
                            "category": category,
                            "title": art.get("title"),
                            "description": art.get("description"),
                            "url": art.get("url"),
                            "publishedAt": art.get("publishedAt"),
                            "content": art.get("content"),
                            "urlToImage": art.get("urlToImage"),
                            "content_hash": generate_hash((art.get("title") or "") + (art.get("url") or "")),
                            "retrieved_date": datetime.now().isoformat()
                        })
                    logger.info(f"  ✓ {len(data.get('articles', []))} articles")
                else:
                    logger.error(f"  ✗ {data.get('message', 'Erreur inconnue')}")
                    if "apiKey" in data.get('message', ''):
                        break
                
                time.sleep(1)  # Rate limiting crucial: 1 requête/seconde
                
            except requests.exceptions.RequestException as e:
                logger.error(f"  ✗ Erreur requête: {e}")
                time.sleep(2)
    
    today = datetime.today().strftime("%Y-%m-%d")
    save_data(articles, f"{RAW_DIR}/newsapi/articles_{today}")
    logger.info(f"[NewsAPI] ✅ Total: {len(articles)} articles")
    return articles

# --------------------------
# 3- Twitter 
# --------------------------
def collect_twitter():
    """Collecte Twitter avec queries optimisées pour volume maximal"""
    
    bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
    if not bearer_token or bearer_token == "AAAAAAAAAAAAAAAAAAAAAA7H4wEAAAAAqtd%2B0vwOm6aaSN8SHEKFDmAm%2Bk4%3D9rynbpBWvMsKJsSi5W6EcxNHkiZmFRteKEmukX312rtnlPxrq1":
        logger.warning("[Twitter] ⚠️ Bearer token manquant ou invalide")
        return []
    
    try:
        client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
        # Test de connexion
        client.get_users_me()
        logger.info("[Twitter] ✓ Connexion établie")
    except Exception as e:
        logger.error(f"[Twitter] ✗ Erreur connexion: {e}")
        return []
    
    # Queries optimisées pour volume et pertinence
    queries = [
        # Actualités générales
        "breaking news", "world news", "latest news",
        # Tech & Innovation
        "AI", "artificial intelligence", "ChatGPT", "machine learning", 
        "blockchain", "crypto", "tech news", "innovation",
        # Business & Finance
        "stocks", "market", "economy", "business", "startup",
        # Science & Environment
        "climate", "science", "space", "research",
        # Spécifique français/Maroc
        "actualité", "politique", "économie", "Maroc",
    ]
    
    tweets_data = []
    request_count = 0
    max_requests = 450  # Limite API: 450 requêtes / 15 min
    
    for query in queries:
        request_count += 1
        
        if request_count >= max_requests:
            logger.warning("[Twitter] ⚠️ Limite de requêtes atteinte")
            break
        
        logger.info(f"[Twitter] ({request_count}/{len(queries)}) '{query}'")
        
        try:
            response = client.search_recent_tweets(
                query=f"{query} -is:retweet -is:reply lang:en OR lang:fr",
                max_results=100,  
                tweet_fields=["created_at", "text", "author_id", "lang", "public_metrics"],
                expansions=["author_id"]
            )
            
            if response.data:
                for tweet in response.data:
                    tweets_data.append({
                        "source_type": "twitter",
                        "query": query,
                        "text": tweet.text,
                        "created_at": tweet.created_at.isoformat() if tweet.created_at else "",
                        "author_id": str(tweet.author_id),
                        "language": tweet.lang if hasattr(tweet, 'lang') else "",
                        "retweet_count": tweet.public_metrics.get('retweet_count', 0) if hasattr(tweet, 'public_metrics') else 0,
                        "like_count": tweet.public_metrics.get('like_count', 0) if hasattr(tweet, 'public_metrics') else 0,
                        "content_hash": generate_hash(tweet.text),
                        "retrieved_date": datetime.now().isoformat()
                    })
                
                logger.info(f"  ✓ {len(response.data)} tweets")
            else:
                logger.info(f"  ⚠️ 0 tweets trouvés")
            
        except tweepy.TooManyRequests:
            logger.warning("  ⚠️ Rate limit atteint, pause de 15 minutes...")
            time.sleep(900)  # 15 minutes
        except tweepy.Forbidden as e:
            logger.error(f"  ✗ Accès interdit: {e}")
            break
        except Exception as e:
            logger.error(f"  ✗ Erreur: {e}")
        
        time.sleep(1)  # Rate limiting entre requêtes
    
    today = datetime.today().strftime("%Y-%m-%d")
    save_data(tweets_data, f"{RAW_DIR}/twitter/tweets_{today}")
    logger.info(f"[Twitter] ✅ Total: {len(tweets_data)} tweets")
    return tweets_data

# --------------------------
# 4️⃣ Reddit - IMPLÉMENTATION COMPLÈTE
# --------------------------
def collect_reddit():
    """Collecte Reddit avec subreddits populaires"""
    
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "NewsCollector/1.0")
    
    if not (client_id and client_secret) or client_id == "votre_client_id_ici":
        logger.warning("[Reddit] ⚠️ Identifiants manquants ou invalides")
        return []
    
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        # Test de connexion
        reddit.user.me()
        logger.info("[Reddit] ✓ Connexion établie")
    except Exception as e:
        logger.error(f"[Reddit] ✗ Erreur connexion: {e}")
        return []
    
    # Subreddits pertinents pour actualités
    subreddits = [
        # Actualités
        "worldnews", "news", "inthenews", "UpliftingNews",
        # Tech
        "technology", "Futurology", "artificial", "MachineLearning",
        # Science
        "science", "space", "environment", "climate",
        # Business
        "business", "economics", "cryptocurrency", "stocks",
        # Régional
        "france", "europe", "Morocco", "Africa"
    ]
    
    posts_data = []
    
    for subreddit_name in subreddits:
        logger.info(f"[Reddit] r/{subreddit_name}")
        
        try:
            subreddit = reddit.subreddit(subreddit_name)
            
            # Collecter hot + new pour maximiser le volume
            collected = 0
            for post in subreddit.hot(limit=50):
                posts_data.append({
                    "source_type": "reddit",
                    "subreddit": subreddit_name,
                    "post_type": "hot",
                    "title": post.title,
                    "selftext": post.selftext[:500] if post.selftext else "",
                    "url": post.url,
                    "score": post.score,
                    "num_comments": post.num_comments,
                    "created_utc": datetime.fromtimestamp(post.created_utc).isoformat(),
                    "author": str(post.author),
                    "content_hash": generate_hash(post.title + post.url),
                    "retrieved_date": datetime.now().isoformat()
                })
                collected += 1
            
            for post in subreddit.new(limit=50):
                posts_data.append({
                    "source_type": "reddit",
                    "subreddit": subreddit_name,
                    "post_type": "new",
                    "title": post.title,
                    "selftext": post.selftext[:500] if post.selftext else "",
                    "url": post.url,
                    "score": post.score,
                    "num_comments": post.num_comments,
                    "created_utc": datetime.fromtimestamp(post.created_utc).isoformat(),
                    "author": str(post.author),
                    "content_hash": generate_hash(post.title + post.url),
                    "retrieved_date": datetime.now().isoformat()
                })
                collected += 1
            
            logger.info(f"  ✓ {collected} posts")
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            logger.error(f"  ✗ Erreur: {e}")
    
    today = datetime.today().strftime("%Y-%m-%d")
    save_data(posts_data, f"{RAW_DIR}/reddit/posts_{today}")
    logger.info(f"[Reddit] ✅ Total: {len(posts_data)} posts")
    return posts_data

# --------------------------
# 5️⃣ Scraping - VERSION SIMPLIFIÉE ET ROBUSTE
# --------------------------
def collect_scraping():
    """Scraping simplifié avec sélecteurs génériques robustes"""
    
    # Headers pour éviter les blocages
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    
    # Sites accessibles avec sélecteurs simples
    sites = {
        "BBC": "https://www.bbc.com/news",
        "Reuters": "https://www.reuters.com/world/",
        "Guardian": "https://www.theguardian.com/international",
        "AlJazeera": "https://www.aljazeera.com",
        "DW": "https://www.dw.com/en/top-stories/s-9097",
        "France24": "https://www.france24.com/en/",
        "APNews": "https://apnews.com/",
        "Euronews": "https://www.euronews.com/news",
    }
    
    all_articles = []
    
    for source, url in sites.items():
        logger.info(f"[Scraping] {source}")
        
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Extraire tous les titres (h1-h4) avec liens
            titles_found = 0
            for tag in soup.find_all(['h1', 'h2', 'h3', 'h4'], limit=100):
                text = tag.get_text(strip=True)
                
                # Filtrer les titres trop courts ou trop longs
                if len(text) < 15 or len(text) > 300:
                    continue
                
                # Chercher le lien associé
                link = ""
                link_tag = tag.find("a") or tag.find_parent("a")
                if link_tag and link_tag.get("href"):
                    href = link_tag["href"]
                    # Construire l'URL complète
                    if href.startswith('http'):
                        link = href
                    elif href.startswith('/'):
                        link = urljoin(url, href)
                    else:
                        link = urljoin(url, '/' + href)
                
                # Éviter les liens internes non-articles
                if link and any(skip in link for skip in ['#', 'javascript:', 'mailto:']):
                    continue
                
                all_articles.append({
                    "source_type": "scraping",
                    "source": source,
                    "title": text,
                    "link": link,
                    "content_hash": generate_hash(text + link),
                    "retrieved_date": datetime.now().isoformat()
                })
                titles_found += 1
            
            logger.info(f"  ✓ {titles_found} articles")
            time.sleep(2)  # Rate limiting important
            
        except requests.exceptions.Timeout:
            logger.error(f"  ✗ Timeout pour {source}")
        except requests.exceptions.RequestException as e:
            logger.error(f"  ✗ Erreur requête: {e}")
        except Exception as e:
            logger.error(f"  ✗ Erreur inattendue: {e}")
    
    today = datetime.today().strftime("%Y-%m-%d")
    save_data(all_articles, f"{RAW_DIR}/scraping/scraped_articles_{today}")
    logger.info(f"[Scraping] ✅ Total: {len(all_articles)} articles")
    return all_articles

# --------------------------
# 6️⃣ Fusion et déduplication
# --------------------------
def combine_all_sources():
    """Combine toutes les sources et élimine les doublons"""
    
    today = datetime.today().strftime("%Y-%m-%d")
    all_data = []
    
    logger.info("[Fusion] Chargement des données collectées...")
    
    # Charger tous les fichiers JSON du jour
    for subdir in ['rss', 'twitter', 'reddit', 'scraping', 'newsapi']:
        subdir_path = f"{RAW_DIR}/{subdir}"
        if not os.path.exists(subdir_path):
            continue
            
        json_files = [f for f in os.listdir(subdir_path) 
                      if f.endswith('.json') and today in f]
        
        for file in json_files:
            try:
                filepath = f"{subdir_path}/{file}"
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.extend(data)
                    logger.info(f"  ✓ {file}: {len(data)} entrées")
            except Exception as e:
                logger.error(f"  ✗ Erreur lecture {file}: {e}")
    
    logger.info(f"[Fusion] Total avant déduplication: {len(all_data)}")
    
    # Déduplication basée sur content_hash
    unique_data = {}
    duplicates = 0
    
    for item in all_data:
        hash_key = item.get('content_hash', generate_hash(str(item)))
        if hash_key not in unique_data:
            unique_data[hash_key] = item
        else:
            duplicates += 1
    
    final_data = list(unique_data.values())
    
    # Sauvegarder la version combinée
    save_data(final_data, f"{RAW_DIR}/combined/all_sources_{today}")
    
    logger.info(f"[Fusion] ✅ {len(final_data)} articles uniques")
    logger.info(f"[Fusion] 🔄 {duplicates} doublons éliminés")
    
    # Statistiques par source
    stats_by_source = {}
    for item in final_data:
        source_type = item.get('source_type', 'unknown')
        stats_by_source[source_type] = stats_by_source.get(source_type, 0) + 1
    
    logger.info("[Fusion] 📊 Répartition par source:")
    for source, count in sorted(stats_by_source.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {source}: {count} ({count/len(final_data)*100:.1f}%)")
    
    return final_data

# --------------------------
# 🚀 Exécution principale
# --------------------------
if __name__ == "__main__":
    
    print("\n" + "="*80)
    print("📰 COLLECTEUR DE DONNÉES OPTIMISÉ - Version améliorée")
    print("="*80 + "\n")
    
    start_time = datetime.now()
    stats = {}
    
    # Phase 1: RSS (TOUJOURS actif - utilise votre feeds.json)
    print("\n🔄 Phase 1/5: Collecte RSS (feeds.json)")
    print("-" * 80)
    try:
        rss_data = collect_rss()
        stats["rss"] = len(rss_data)
    except Exception as e:
        logger.error(f"❌ Erreur RSS: {e}")
        stats["rss"] = 0
    
    # Phase 2: NewsAPI (si clé disponible)
    print("\n🔄 Phase 2/5: Collecte NewsAPI")
    print("-" * 80)
    try:
        newsapi_data = collect_newsapi()
        stats["newsapi"] = len(newsapi_data)
    except Exception as e:
        logger.error(f"❌ Erreur NewsAPI: {e}")
        stats["newsapi"] = 0
    
    # Phase 3: Twitter (si token disponible)
    print("\n🔄 Phase 3/5: Collecte Twitter")
    print("-" * 80)
    try:
        twitter_data = collect_twitter()
        stats["twitter"] = len(twitter_data)
    except Exception as e:
        logger.error(f"❌ Erreur Twitter: {e}")
        stats["twitter"] = 0
    
    # Phase 4: Reddit (si identifiants disponibles)
    print("\n🔄 Phase 4/5: Collecte Reddit")
    print("-" * 80)
    try:
        reddit_data = collect_reddit()
        stats["reddit"] = len(reddit_data)
    except Exception as e:
        logger.error(f"❌ Erreur Reddit: {e}")
        stats["reddit"] = 0
    
    # Phase 5: Scraping (TOUJOURS actif)
    print("\n🔄 Phase 5/5: Web Scraping")
    print("-" * 80)
    try:
        scraping_data = collect_scraping()
        stats["scraping"] = len(scraping_data)
    except Exception as e:
        logger.error(f"❌ Erreur Scraping: {e}")
        stats["scraping"] = 0
    
    # Phase 6: Fusion finale
    print("\n🔄 Phase 6: Fusion et déduplication")
    print("-" * 80)
    try:
        combined_data = combine_all_sources()
    except Exception as e:
        logger.error(f"❌ Erreur fusion: {e}")
    
    # Rapport final
    duration = (datetime.now() - start_time).total_seconds()
    total = sum(stats.values())
    
    print("\n" + "="*80)
    print("✅ COLLECTE TERMINÉE")
    print("="*80)
    print(f"⏱️  Durée totale: {duration:.1f} secondes ({duration/60:.1f} minutes)")
    print(f"📊 Total collecté: {total:,} éléments\n")
    print("Détails par source:")
    print("-" * 80)
    for source, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total * 100) if total > 0 else 0
        print(f"   {source.upper():15s} : {count:6,} ({percentage:5.1f}%)")
    print("\n" + "="*80)
    print("✨ Données brutes prêtes pour le prétraitement!")
    print(f"📁 Emplacement: {RAW_DIR}/")
    print("="*80 + "\n")