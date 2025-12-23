"""
Version TEMPORAIRE de collect_newsapi() pour forcer la collecte
À utiliser UNE FOIS pour tester si NewsAPI fonctionne vraiment
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



# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
def collect_newsapi_force():
    """
    Collecte NewsAPI SANS AUCUN FILTRE
    Pour tester si l'API fonctionne et voir quels articles sont disponibles
    """
    
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        logger.warning("[NewsAPI] Clé API manquante dans .env")
        return []

    articles = []
    
    # Plus de pays et catégories
    countries = ['us', 'fr', 'gb', 'de', 'ca', 'au']
    categories = ['general', 'technology', 'business', 'science']

    logger.info(f"[NewsAPI] MODE TEST - SANS FILTRES")
    
    request_count = 0
    max_requests = 8  # Limiter pour ne pas gaspiller l'API

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
                'pageSize': 20  # Seulement 20 pour tester
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get('status') == 'ok':
                    raw_articles = data.get("articles", [])
                    logger.info(f"  [API] {len(raw_articles)} articles reçus")
                    
                    # PAS DE FILTRES - tout garder
                    for art in raw_articles:
                        articles.append({
                            "source_type": "newsapi_test",
                            "source": art.get("source", {}).get("name", "Unknown"),
                            "country": country,
                            "category": category,
                            "title": art.get("title"),
                            "description": art.get("description"),
                            "url": art.get("url"),
                            "publishedAt": art.get("publishedAt"),
                            "content": art.get("content"),
                            "news_type": category,
                            "retrieved_date": datetime.now(timezone.utc).isoformat()
                        })
                    
                    logger.info(f"  [OK] {len(raw_articles)} articles gardés (TOUS)")
                    
                else:
                    logger.error(f"  [ERREUR] {data.get('message', 'Erreur inconnue')}")

                time.sleep(1)

            except Exception as e:
                logger.error(f"  [ERREUR]: {e}")

    # Sauvegarde
    today = datetime.now().strftime("%Y-%m-%d_%H%M")
    save_data(articles, os.path.join(RAW_DIR, "newsapi", f"TEST_no_filters_{today}"))
    
    logger.info(f"[NewsAPI TEST] Total: {len(articles)} articles (sans filtres)")
    
    # Afficher un échantillon
    if articles:
        logger.info(f"\n📰 ÉCHANTILLON (5 premiers articles):")
        for i, art in enumerate(articles[:5], 1):
            logger.info(f"   {i}. [{art['country']}/{art['category']}] {art['title'][:60]}...")
            logger.info(f"      Publié: {art['publishedAt']}")
    
    return articles


# SCRIPT DE TEST STANDALONE
if __name__ == "__main__":
    """
    Pour tester NewsAPI directement sans lancer toute la collecte
    """
    
    print("\n" + "="*80)
    print("TEST NEWSAPI - MODE SANS FILTRES")
    print("="*80 + "\n")
    
    # Imports nécessaires
    import os
    import requests
    from datetime import datetime, timezone
    from dotenv import load_dotenv
    import time
    
    load_dotenv()
    
    # Variables globales minimales
    RAW_DIR = "../data/raw"
    os.makedirs(os.path.join(RAW_DIR, "newsapi"), exist_ok=True)
    
    # Fonction simplifiée save_data
    def save_data(data, filename, format='json'):
        import json
        with open(f"{filename}.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Sauvegardé: {filename}.json")
    
    # Lancer la collecte test
    articles = collect_newsapi_force()
    
    print("\n" + "="*80)
    print("RÉSULTATS DU TEST")
    print("="*80)
    
    if articles:
        print(f"✅ NewsAPI FONCTIONNE")
        print(f"   {len(articles)} articles collectés")
        print(f"\n   → Votre clé API est valide")
        print(f"   → Le problème vient des FILTRES (date/doublons)")
        
        # Analyser les dates
        dates = []
        for art in articles:
            if art.get('publishedAt'):
                try:
                    date = datetime.fromisoformat(art['publishedAt'].replace('Z', '+00:00'))
                    dates.append(date)
                except:
                    pass
        
        if dates:
            oldest = min(dates)
            newest = max(dates)
            now = datetime.now(timezone.utc)
            
            print(f"\n📅 DATES DES ARTICLES:")
            print(f"   Plus récent : {newest.strftime('%Y-%m-%d %H:%M')} ({(now-newest).total_seconds()/3600:.1f}h)")
            print(f"   Plus ancien : {oldest.strftime('%Y-%m-%d %H:%M')} ({(now-oldest).total_seconds()/3600:.1f}h)")
            
            hours_24 = sum(1 for d in dates if (now - d).total_seconds() < 24*3600)
            print(f"\n   Articles < 24h : {hours_24}/{len(dates)}")
            
            if hours_24 == 0:
                print(f"\n   ⚠️  PROBLÈME IDENTIFIÉ:")
                print(f"      Tous les articles ont plus de 24h")
                print(f"      → Augmentez HOURS_BACK = 72 dans le script")
    else:
        print(f"❌ AUCUN ARTICLE COLLECTÉ")
        print(f"\n   Vérifiez:")
        print(f"   1. Votre clé API dans .env")
        print(f"   2. Votre connexion internet")
        print(f"   3. Les logs d'erreur ci-dessus")
    
    print("="*80 + "\n")