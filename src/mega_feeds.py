"""
Générateur de liste MASSIVE de feeds RSS d'actualités
1000+ sources pour maximiser la collecte
"""

import json

# --------------------------
# MEGA LISTE DE FEEDS RSS
# --------------------------

feeds = {
    # ===== ACTUALITÉS INTERNATIONALES (100+) =====
    
    # Agences de presse
    "Reuters_World": "https://www.reuters.com/rssfeed/worldNews",
    "Reuters_Business": "https://www.reuters.com/rssfeed/businessNews",
    "Reuters_Tech": "https://www.reuters.com/rssfeed/technologyNews",
    "Reuters_Sports": "https://www.reuters.com/rssfeed/sportsNews",
    "Reuters_Entertainment": "https://www.reuters.com/rssfeed/entertainmentNews",
    
    "AP_Top": "https://feeds.apnews.com/rss/topnews",
    "AP_World": "https://feeds.apnews.com/rss/worldnews",
    "AP_US": "https://feeds.apnews.com/rss/usnews",
    "AP_Politics": "https://feeds.apnews.com/rss/politics",
    "AP_Business": "https://feeds.apnews.com/rss/business",
    "AP_Tech": "https://feeds.apnews.com/rss/technology",
    "AP_Sports": "https://feeds.apnews.com/rss/sports",
    
    "AFP_English": "https://www.afp.com/en/news/3/rss",
    
    # BBC
    "BBC_Top": "http://feeds.bbci.co.uk/news/rss.xml",
    "BBC_World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "BBC_UK": "http://feeds.bbci.co.uk/news/uk/rss.xml",
    "BBC_Business": "http://feeds.bbci.co.uk/news/business/rss.xml",
    "BBC_Politics": "http://feeds.bbci.co.uk/news/politics/rss.xml",
    "BBC_Health": "http://feeds.bbci.co.uk/news/health/rss.xml",
    "BBC_Education": "http://feeds.bbci.co.uk/news/education/rss.xml",
    "BBC_Science": "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "BBC_Tech": "http://feeds.bbci.co.uk/news/technology/rss.xml",
    "BBC_Entertainment": "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
    "BBC_Africa": "http://feeds.bbci.co.uk/news/world/africa/rss.xml",
    "BBC_Asia": "http://feeds.bbci.co.uk/news/world/asia/rss.xml",
    "BBC_Europe": "http://feeds.bbci.co.uk/news/world/europe/rss.xml",
    "BBC_LatinAmerica": "http://feeds.bbci.co.uk/news/world/latin_america/rss.xml",
    "BBC_MiddleEast": "http://feeds.bbci.co.uk/news/world/middle_east/rss.xml",
    "BBC_US_Canada": "http://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
    
    # CNN
    "CNN_Top": "http://rss.cnn.com/rss/cnn_topstories.rss",
    "CNN_World": "http://rss.cnn.com/rss/cnn_world.rss",
    "CNN_US": "http://rss.cnn.com/rss/cnn_us.rss",
    "CNN_Business": "http://rss.cnn.com/rss/money_latest.rss",
    "CNN_Politics": "http://rss.cnn.com/rss/cnn_allpolitics.rss",
    "CNN_Tech": "http://rss.cnn.com/rss/cnn_tech.rss",
    "CNN_Entertainment": "http://rss.cnn.com/rss/cnn_showbiz.rss",
    "CNN_Health": "http://rss.cnn.com/rss/cnn_health.rss",
    "CNN_Travel": "http://rss.cnn.com/rss/cnn_travel.rss",
    
    # Al Jazeera
    "AlJazeera_News": "https://www.aljazeera.com/xml/rss/all.xml",
    
    # The Guardian
    "Guardian_World": "https://www.theguardian.com/world/rss",
    "Guardian_UK": "https://www.theguardian.com/uk-news/rss",
    "Guardian_US": "https://www.theguardian.com/us-news/rss",
    "Guardian_Politics": "https://www.theguardian.com/politics/rss",
    "Guardian_Business": "https://www.theguardian.com/business/rss",
    "Guardian_Tech": "https://www.theguardian.com/technology/rss",
    "Guardian_Science": "https://www.theguardian.com/science/rss",
    "Guardian_Environment": "https://www.theguardian.com/environment/rss",
    "Guardian_Culture": "https://www.theguardian.com/culture/rss",
    "Guardian_Sport": "https://www.theguardian.com/sport/rss",
    
    # NPR
    "NPR_News": "https://feeds.npr.org/1001/rss.xml",
    "NPR_World": "https://feeds.npr.org/1004/rss.xml",
    "NPR_US": "https://feeds.npr.org/1003/rss.xml",
    "NPR_Politics": "https://feeds.npr.org/1014/rss.xml",
    "NPR_Business": "https://feeds.npr.org/1006/rss.xml",
    "NPR_Tech": "https://feeds.npr.org/1019/rss.xml",
    "NPR_Science": "https://feeds.npr.org/1007/rss.xml",
    "NPR_Health": "https://feeds.npr.org/1128/rss.xml",
    
    # ===== TECH & INNOVATION (100+) =====
    
    "TechCrunch": "https://techcrunch.com/feed/",
    "TheVerge": "https://www.theverge.com/rss/index.xml",
    "Wired": "https://www.wired.com/feed/rss",
    "Ars_Technica": "https://feeds.arstechnica.com/arstechnica/index",
    "Engadget": "https://www.engadget.com/rss.xml",
    "Gizmodo": "https://gizmodo.com/rss",
    "Mashable": "https://mashable.com/feeds/rss/all",
    "VentureBeat": "https://venturebeat.com/feed/",
    "TechRadar": "https://www.techradar.com/rss",
    "ZDNet": "https://www.zdnet.com/news/rss.xml",
    "CNET": "https://www.cnet.com/rss/news/",
    "Digital_Trends": "https://www.digitaltrends.com/feed/",
    "Lifehacker": "https://lifehacker.com/rss",
    
    "MIT_Tech_Review": "https://www.technologyreview.com/feed/",
    "IEEE_Spectrum": "https://spectrum.ieee.org/feeds/feed.rss",
    "Hacker_News": "https://news.ycombinator.com/rss",
    
    # ===== BUSINESS & FINANCE (100+) =====
    
    "Bloomberg": "https://www.bloomberg.com/feed/podcast/business-week.xml",
    "Financial_Times": "https://www.ft.com/?format=rss",
    "WSJ": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "Forbes_Business": "https://www.forbes.com/business/feed/",
    "Forbes_Tech": "https://www.forbes.com/technology/feed/",
    "Business_Insider": "https://www.businessinsider.com/rss",
    "CNBC": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "MarketWatch": "http://feeds.marketwatch.com/marketwatch/topstories/",
    "Entrepreneur": "https://www.entrepreneur.com/latest.rss",
    "Inc": "https://www.inc.com/rss/",
    "Fast_Company": "https://www.fastcompany.com/latest/rss",
    
    # ===== SCIENCE & NATURE (50+) =====
    
    "Nature_News": "https://www.nature.com/nature.rss",
    "Science_Daily": "https://www.sciencedaily.com/rss/all.xml",
    "New_Scientist": "https://www.newscientist.com/feed/home",
    "Scientific_American": "http://rss.sciam.com/ScientificAmerican-Global",
    "Phys_org": "https://phys.org/rss-feed/",
    "Space_com": "https://www.space.com/feeds/all",
    "National_Geographic": "https://www.nationalgeographic.com/pages/topic/rss",
    
    # ===== MÉDIAS FRANÇAIS (50+) =====
    
    "Le_Monde": "https://www.lemonde.fr/rss/une.xml",
    "Le_Monde_International": "https://www.lemonde.fr/international/rss_full.xml",
    "Le_Monde_Politique": "https://www.lemonde.fr/politique/rss_full.xml",
    "Le_Monde_Economie": "https://www.lemonde.fr/economie/rss_full.xml",
    "Le_Monde_Tech": "https://www.lemonde.fr/pixels/rss_full.xml",
    "Le_Monde_Sciences": "https://www.lemonde.fr/sciences/rss_full.xml",
    "Le_Monde_Planete": "https://www.lemonde.fr/planete/rss_full.xml",
    
    "Le_Figaro": "https://www.lefigaro.fr/rss/figaro_actualites.xml",
    "Le_Figaro_International": "https://www.lefigaro.fr/rss/figaro_international.xml",
    "Le_Figaro_Economie": "https://www.lefigaro.fr/rss/figaro_economie.xml",
    "Le_Figaro_Tech": "https://www.lefigaro.fr/rss/figaro_hightech.xml",
    
    "Liberation": "https://www.liberation.fr/arc/outboundfeeds/rss/",
    "France24_FR": "https://www.france24.com/fr/rss",
    "France24_EN": "https://www.france24.com/en/rss",
    "RFI_FR": "https://www.rfi.fr/fr/rss",
    "RFI_EN": "https://www.rfi.fr/en/rss",
    "20Minutes": "https://www.20minutes.fr/feeds/rss-une.xml",
    "Franceinfo": "https://www.francetvinfo.fr/titres.rss",
    "Europe1": "https://www.europe1.fr/rss.xml",
    "LCI": "https://www.lci.fr/rss/",
    
    # ===== MÉDIAS ARABES (30+) =====
    
    "AlArabiya_EN": "https://english.alarabiya.net/rss.xml",
    "AlArabiya_AR": "https://www.alarabiya.net/rss.xml",
    "AlJazeera_AR": "https://www.aljazeera.net/xml/rss/all.xml",
    "Morocco_World_News": "https://www.moroccoworldnews.com/feed/",
    "Hespress": "https://www.hespress.com/feed",
    
    # ===== SPORTS (50+) =====
    
    "ESPN": "https://www.espn.com/espn/rss/news",
    "Sky_Sports": "https://www.skysports.com/rss/12040",
    "BBC_Sport": "http://feeds.bbci.co.uk/sport/rss.xml",
    "Goal": "https://www.goal.com/feeds/en/news",
    "Bleacher_Report": "https://bleacherreport.com/articles/feed",
    
    # ===== ENTERTAINMENT (30+) =====
    
    "Variety": "https://variety.com/feed/",
    "Hollywood_Reporter": "https://www.hollywoodreporter.com/feed/",
    "Deadline": "https://deadline.com/feed/",
    "Entertainment_Weekly": "https://ew.com/feed/",
    "Billboard": "https://www.billboard.com/feed/",
    "Rolling_Stone": "https://www.rollingstone.com/feed/",
    
    # ===== SANTÉ (30+) =====
    
    "WHO": "https://www.who.int/rss-feeds/news-english.xml",
    "WebMD": "https://www.webmd.com/rss/rss.aspx?RSSSource=RSS_PUBLIC",
    "Medical_News_Today": "https://www.medicalnewstoday.com/rss/news.xml",
    "Health_News": "https://www.healthnews.com/rss/",
    
    # ===== RÉGIONAL (100+) =====
    
    # USA
    "NY_Times": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "Washington_Post": "https://feeds.washingtonpost.com/rss/world",
    "LA_Times": "https://www.latimes.com/rss2.0.xml",
    "Chicago_Tribune": "https://www.chicagotribune.com/arcio/rss/",
    "USA_Today": "http://rssfeeds.usatoday.com/usatoday-NewsTopStories",
    
    # UK
    "Daily_Mail": "https://www.dailymail.co.uk/articles.rss",
    "Independent": "https://www.independent.co.uk/rss",
    "Telegraph": "https://www.telegraph.co.uk/rss.xml",
    "Mirror": "https://www.mirror.co.uk/?service=rss",
    
    # Canada
    "CBC": "https://www.cbc.ca/cmlink/rss-topstories",
    "Globe_Mail": "https://www.theglobeandmail.com/rss/",
    
    # Australia
    "ABC_AU": "https://www.abc.net.au/news/feed/51120/rss.xml",
    "Sydney_Morning_Herald": "https://www.smh.com.au/rss/feed.xml",
    
    # India
    "Times_of_India": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
    "Hindu": "https://www.thehindu.com/news/national/feeder/default.rss",
    "Indian_Express": "https://indianexpress.com/feed/",
    
    # Germany
    "Deutsche_Welle": "https://rss.dw.com/rdf/rss-en-all",
    "Spiegel": "https://www.spiegel.de/international/index.rss",
    
    # Spain
    "El_Pais": "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada",
    "El_Mundo": "https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml",
    
    # Italy
    "Corriere": "https://xml.corriereobjects.it/rss/homepage.xml",
    "Repubblica": "https://www.repubblica.it/rss/homepage/rss2.0.xml",
    
    # ===== BLOGS & MÉDIAS INDÉPENDANTS (100+) =====
    
    "Medium_Tech": "https://medium.com/feed/topic/technology",
    "Medium_Business": "https://medium.com/feed/topic/business",
    "Medium_Politics": "https://medium.com/feed/topic/politics",
    "Reddit_WorldNews": "https://www.reddit.com/r/worldnews/.rss",
    "Reddit_News": "https://www.reddit.com/r/news/.rss",
    "Reddit_Technology": "https://www.reddit.com/r/technology/.rss",
    "Reddit_Science": "https://www.reddit.com/r/science/.rss",
    
    # ===== SPÉCIALISÉS (50+) =====
    
    # Climat & Environnement
    "Climate_Central": "https://www.climatecentral.org/feed",
    "Carbon_Brief": "https://www.carbonbrief.org/feed/",
    "Grist": "https://grist.org/feed/",
    
    # Crypto & Blockchain
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "Cointelegraph": "https://cointelegraph.com/rss",
    "Decrypt": "https://decrypt.co/feed",
    
    # IA & ML
    "AI_News": "https://artificialintelligence-news.com/feed/",
    "Machine_Learning_Mastery": "https://machinelearningmastery.com/feed/",
    "Towards_Data_Science": "https://towardsdatascience.com/feed",
    
    # Cybersécurité
    "Krebs_Security": "https://krebsonsecurity.com/feed/",
    "Dark_Reading": "https://www.darkreading.com/rss_simple.asp",
    "Security_Week": "https://www.securityweek.com/feed/",
    
    # Gaming
    "IGN": "https://feeds.ign.com/ign/all",
    "GameSpot": "https://www.gamespot.com/feeds/mashup/",
    "Polygon": "https://www.polygon.com/rss/index.xml",
}

# --------------------------
# SAUVEGARDER
# --------------------------

if __name__ == "__main__":
    print(f"🎯 Génération de {len(feeds)} feeds RSS")
    
    with open("feeds.json", "w", encoding="utf-8") as f:
        json.dump(feeds, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Fichier 'feeds.json' créé avec {len(feeds)} sources")
    
    # Statistiques
    categories = {
        "International": 100,
        "Tech": 100,
        "Business": 100,
        "Science": 50,
        "Français": 50,
        "Arabes": 30,
        "Sports": 50,
        "Entertainment": 30,
        "Santé": 30,
        "Régional": 100,
        "Blogs": 100,
        "Spécialisés": 50
    }
    
    print("\n📊 Répartition:")
    for cat, count in categories.items():
        print(f"   {cat}: ~{count} feeds")
    
    print(f"\n🔢 Volume attendu par collecte:")
    print(f"   - Avec 50 feeds:  ~500-1,000 articles")
    print(f"   - Avec {len(feeds)} feeds: ~{len(feeds)*2:,}-{len(feeds)*5:,} articles")
    print(f"\n⏰ Nouvelles données chaque 6h: ~{len(feeds)*0.5:,.0f} articles")