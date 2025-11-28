import streamlit as st
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import re

# Configuration
st.set_page_config(page_title="Analyse de Tendances Médiatiques", layout="wide")
st.title("📊 Analyse de Tendances Médiatiques")
st.markdown("Découvrez les tendances actuelles et explorez les articles par recherche sémantique")

@st.cache_data
def load_data():
    try:
        # Chargement des données de base
        df = pd.read_csv('dashboard_data.csv')
        embeddings = np.load('dashboard_embeddings.npy')
        
        # Modèle standard
        sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        st.success(f'✅ Données chargées: {len(df)} articles')
        return df, embeddings, sentence_model
        
    except Exception as e:
        st.error(f"❌ Erreur de chargement: {e}")
        return None, None, None

# Fonction pour analyser les tendances des topics
def analyze_topic_trends(df, top_categories):
    """Analyse l'évolution temporelle des 3 topics principaux"""
    topic_trends = {}
    
    if 'published' not in df.columns or 'news_type' not in df.columns:
        return topic_trends
    
    try:
        # Convertir la date de publication
        df['published_datetime'] = pd.to_datetime(df['published'], errors='coerce')
        df['published_date'] = df['published_datetime'].dt.date
        
        # Analyser chaque topic
        for i, (category, count) in enumerate(top_categories.items()):
            if i >= 3:  # Seulement les 3 premiers
                break
                
            # Filtrer les articles de cette catégorie
            category_df = df[df['news_type'] == category]
            
            if len(category_df) > 0:
                # Évolution quotidienne
                daily_trend = category_df['published_date'].value_counts().sort_index()
                
                # Statistiques temporelles
                latest_article = category_df['published_datetime'].max()
                oldest_article = category_df['published_datetime'].min()
                articles_last_7_days = len(category_df[category_df['published_datetime'] >= (latest_article - timedelta(days=7))])
                
                topic_trends[category] = {
                    'daily_trend': daily_trend,
                    'total_articles': len(category_df),
                    'latest_article': latest_article,
                    'oldest_article': oldest_article,
                    'articles_last_7_days': articles_last_7_days,
                    'trend_direction': '📈' if articles_last_7_days > len(category_df) / 30 else '📊'
                }
                
    except Exception as e:
        print(f"⚠️ Erreur analyse topics: {e}")
    
    return topic_trends

# Fonction pour analyser les tendances générales
def analyze_trends(df):
    trends = {}
    
    # Tendances par catégorie
    if 'news_type' in df.columns:
        top_categories = df['news_type'].value_counts().head(5)
        trends['top_categories'] = top_categories
        
        # Analyser les tendances temporelles des topics
        trends['topic_trends'] = analyze_topic_trends(df, top_categories)
    
    # Tendances par langue
    if 'language' in df.columns:
        trends['top_languages'] = df['language'].value_counts().head(5)
    
    # Tendances temporelles générales
    if 'published' in df.columns:
        try:
            df['published_datetime'] = pd.to_datetime(df['published'], errors='coerce')
            df['published_date'] = df['published_datetime'].dt.date
            
            # Tendances quotidiennes
            daily_counts = df['published_date'].value_counts().sort_index()
            trends['daily_trend'] = daily_counts
            
            # Dernières 24 heures
            latest_date = df['published_datetime'].max()
            last_24h = df[df['published_datetime'] >= (latest_date - timedelta(days=1))]
            trends['last_24h_count'] = len(last_24h)
            
        except Exception as e:
            trends['daily_trend'] = None
    
    return trends

# Fonction pour détecter les top topics
def get_top_topics(df, topic_trends=None):
    topics = []
    
    # Analyser les catégories principales avec tendances
    if 'news_type' in df.columns:
        top_cats = df['news_type'].value_counts().head(3)
        for cat, count in top_cats.items():
            trend_info = ""
            if topic_trends and cat in topic_trends:
                trend_data = topic_trends[cat]
                trend_info = f"+{trend_data['articles_last_7_days']} 7j"
                
            topics.append({
                'id': f"cat_{cat}",
                'name': f"{cat.title()}",
                'count': count,
                'trend': trend_info,
                'icon': "📈" if trend_info and trend_data['articles_last_7_days'] > 10 else "📊"
            })
    
    return topics

# Chargement
df, embeddings, sentence_model = load_data()

if df is not None:
    # Analyse des tendances
    trends = analyze_trends(df)
    
    # Sidebar
    st.sidebar.header("🔍 Recherche & Filtres")
    
    # Recherche sémantique
    search_query = st.sidebar.text_input("Recherche sémantique:", placeholder="Ex: intelligence artificielle, économie...")
    
    # Filtres
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if 'news_type' in df.columns:
            categories = ['Toutes'] + list(df['news_type'].unique())
            selected_category = st.selectbox("📂 Catégorie", categories)
        else:
            selected_category = 'Toutes'
    
    with col2:
        if 'language' in df.columns:
            languages = ['Toutes'] + list(df['language'].unique())
            selected_language = st.selectbox("🌐 Langue", languages)
        else:
            selected_language = 'Toutes'

    # Section principale : Tableau de bord des tendances
    st.header("📈 Tableau de Bord des Tendances")
    
    # Métriques principales
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Articles Totaux", f"{len(df):,}")
    with col2:
        if 'news_type' in df.columns:
            st.metric("🏷️ Catégories", df['news_type'].nunique())
    with col3:
        if 'language' in df.columns:
            st.metric("🌐 Langues", df['language'].nunique())
    with col4:
        if 'last_24h_count' in trends:
            st.metric("🕐 24h", trends['last_24h_count'])
        else:
            st.metric("📌 Topics", "Analysés")

    # Top 3 Topics Tendances avec évolution
    st.subheader("🔥 Évolution des 3 Principaux Topics")
    top_topics = get_top_topics(df, trends.get('topic_trends', {}))
    
    if top_topics:
        topic_cols = st.columns(3)
        for i, topic in enumerate(top_topics[:3]):
            with topic_cols[i]:
                st.metric(
                    label=topic['name'],
                    value=f"{topic['count']} articles",
                    delta=topic['trend']
                )
    else:
        st.info("📝 Aucune donnée de topics disponible")

    # NOUVELLE SECTION : Analyse Temporelle des Topics
    if 'topic_trends' in trends and trends['topic_trends']:
        st.subheader("📅 Évolution Temporelle des Topics")
        
        # Créer un graphique comparatif
        trend_data = []
        for category, data in trends['topic_trends'].items():
            if len(data['daily_trend']) > 0:
                for date, count in data['daily_trend'].items():
                    trend_data.append({
                        'Date': date,
                        'Articles': count,
                        'Topic': category
                    })
        
        if trend_data:
            trend_df = pd.DataFrame(trend_data)
            
            # Graphique d'évolution comparée
            fig_comparison = px.line(
                trend_df, 
                x='Date', 
                y='Articles', 
                color='Topic',
                title="Évolution Comparée des 3 Principaux Topics",
                labels={'Date': 'Date', 'Articles': "Nombre d'articles"}
            )
            st.plotly_chart(fig_comparison, use_container_width=True)
            
            # Graphiques individuels
            st.subheader("📊 Détail par Topic")
            detail_cols = st.columns(3)
            
            for i, (category, data) in enumerate(list(trends['topic_trends'].items())[:3]):
                with detail_cols[i]:
                    st.write(f"**{category.title()}**")
                    
                    # Graphique individuel
                    if len(data['daily_trend']) > 0:
                        fig_indiv = px.area(
                            x=data['daily_trend'].index,
                            y=data['daily_trend'].values,
                            title=f"Évolution de {category}",
                            labels={'x': 'Date', 'y': 'Articles'}
                        )
                        st.plotly_chart(fig_indiv, use_container_width=True)
                    

    # Visualisations des tendances générales
    st.subheader("📊 Analyse des Tendances Générales")
    
    trend_tab1, trend_tab2, trend_tab3 = st.tabs(["📂 Par Catégorie", "🌐 Par Langue", "📅 Évolution Générale"])
    
    with trend_tab1:
        if 'top_categories' in trends and len(trends['top_categories']) > 0:
            fig1 = px.bar(
                x=trends['top_categories'].index, 
                y=trends['top_categories'].values,
                title="Distribution des Articles par Catégorie",
                labels={'x': 'Catégorie', 'y': "Nombre d'articles"},
                color=trends['top_categories'].values
            )
            st.plotly_chart(fig1, use_container_width=True)
    
    with trend_tab2:
        if 'top_languages' in trends and len(trends['top_languages']) > 0:
            fig2 = px.pie(
                values=trends['top_languages'].values,
                names=trends['top_languages'].index,
                title="Répartition par Langue"
            )
            st.plotly_chart(fig2, use_container_width=True)
    
    with trend_tab3:
        if 'daily_trend' in trends and trends['daily_trend'] is not None:
            fig3 = px.line(
                x=trends['daily_trend'].index,
                y=trends['daily_trend'].values,
                title="Évolution du Volume d'Articles par Jour",
                labels={'x': 'Date', 'y': "Nombre d'articles"}
            )
            st.plotly_chart(fig3, use_container_width=True)

    # Section recherche sémantique
    st.header("🔍 Recherche Sémantique Avancée")
    
    # Appliquer les filtres pour la recherche
    filtered_df = df.copy()
    if selected_category != 'Toutes':
        filtered_df = filtered_df[filtered_df['news_type'] == selected_category]
    if selected_language != 'Toutes':
        filtered_df = filtered_df[filtered_df['language'] == selected_language]

    if search_query:
        st.subheader(f"🔍 Résultats pour: '{search_query}'")
        st.info(f"📋 Filtres appliqués: {selected_category} | {selected_language}")
        
        with st.spinner('Analyse sémantique en cours...'):
            # Recherche sémantique
            query_embedding = sentence_model.encode([search_query])
            similarities = cosine_similarity(query_embedding, embeddings)[0]
            
            # Filtrer les résultats par catégorie ET langue
            valid_indices = []
            filtered_similarities = []
            
            for idx, similarity in enumerate(similarities):
                article = df.iloc[idx]
                
                # Vérifier si l'article correspond aux filtres
                category_ok = (selected_category == 'Toutes') or (article.get('news_type') == selected_category)
                language_ok = (selected_language == 'Toutes') or (article.get('language') == selected_language)
                
                if category_ok and language_ok:
                    valid_indices.append(idx)
                    filtered_similarities.append(similarity)
            
            # Trier par similarité
            if valid_indices:
                filtered_similarities = np.array(filtered_similarities)
                top_filtered_indices = filtered_similarities.argsort()[::-1][:10]
                
                # Afficher les résultats filtrés
                for pos, original_idx in enumerate(top_filtered_indices):
                    actual_idx = valid_indices[original_idx]
                    similarity_score = filtered_similarities[original_idx]
                    article = df.iloc[actual_idx]
                    
                    with st.expander(f"📄 {article.get('title', 'Sans titre')} (Score: {similarity_score:.3f})"):
                        content = article.get('content', '')
                        if pd.isna(content) or content == '':
                            content = article.get('summary', 'Aucun contenu disponible')
                        
                        st.write(str(content)[:500] + "..." if len(str(content)) > 500 else content)
                        
                        # Métadonnées avec date de publication
                        meta_col1, meta_col2, meta_col3, meta_col4 = st.columns(4)
                        if 'news_type' in article:
                            meta_col1.metric("📂 Catégorie", article['news_type'])
                        if 'language' in article:
                            meta_col2.metric("🌐 Langue", article['language'])
                        if 'topic' in article:
                            meta_col3.metric("📌 Topic", article['topic'])
                        if 'published' in article:
                            try:
                                pub_date = pd.to_datetime(article['published']).strftime('%d/%m/%Y %H:%M')
                                meta_col4.metric("📅 Publié", pub_date)
                            except:
                                meta_col4.metric("📅 Publié", article['published'][:10])
            else:
                st.warning("❌ Aucun résultat trouvé avec les filtres actuels")
    else:
        st.info("👆 Entrez un mot-clé pour lancer une recherche sémantique")

# Footer
st.sidebar.markdown("---")
st.sidebar.info("""
🎯 **Conseils d'utilisation:**
- Analysez l'évolution des topics dans le temps
- Comparez les tendances entre catégories
- Identifiez les sujets émergents
""")