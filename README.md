# 📰 News Trend Analysis – Detecting & Visualizing News Trends

## 📌 Project Overview

**News Trend Analysis** is a Big Data and NLP-based project designed to automatically detect, analyze, and visualize emerging trends from online news sources.  
With the massive growth of digital information, tracking popular topics manually has become challenging. This project proposes an automated pipeline to extract meaningful insights from heterogeneous news data.

The system collects data from multiple platforms, processes it using distributed technologies, applies NLP and machine learning techniques, and presents trends through interactive visualizations.

---

## 🎯 Objectives

- Collect and centralize news data from multiple online sources
- Build a complete **Big Data pipeline**
- Clean, preprocess, and vectorize textual data using **NLP**
- Identify key topics using **topic modeling and clustering**
- Analyze the **temporal evolution of trends**
- Provide clear and interactive visualizations

---

## 🏗️ Global Architecture

The pipeline follows these main steps:

1. **Data Collection**
2. **Distributed Storage**
3. **Preprocessing & NLP**
4. **Trend Detection & Analysis**
5. **Visualization**

---

## 📥 Data Sources

The project gathers news data every 24 hours from:

- **RSS Feeds**  
  Aggregated news from multiple platforms

- **Web Scraping**  
  Targeted scraping of major news websites:
  - BBC
  - CNN
  - Reuters
  - Le Monde
  - etc.

- **Reddit API**  
  Specialized subreddits such as:
  - `worldnews`
  - `news`
  - `tech`  

  Collected fields include:
  - Title
  - URL
  - Score
  - Creation date
  - Automatically assigned category

---

## 🧹 Preprocessing & Methodology

### 1. Text Preprocessing (Apache Spark / PySpark)

- Text cleaning and normalization
- Tokenization and lemmatization
- Combination of article title and content
- Storage of processed data in **HDFS**

### 2. Trend Analysis

- Topic extraction using **BERTopic**
- Semantic analysis with **SentenceTransformer**
- Clustering and classification to detect patterns
- Results stored in **HDFS** for further analysis

---

## 📊 Analysis Results

The system provides several types of insights:

### 🔝 Top Trends & Topics
- Identification of trending topics (e.g., Technology, Politics, General News)

### 🔎 Semantic Search
- Filtering and searching articles by semantic similarity
- Exploration of relevant articles per topic

### 🧠 Classification & Clustering
- Detection of similar article groups
- Models used:
  - Random Forest
  - Support Vector Machine (SVM)
  - XGBoost

### ⏱️ Temporal Evolution
- Time-series visualizations showing topic popularity
- Detection of emerging and declining trends

---

## 🗄️ Distributed Storage & Containerization

- **HDFS (Hadoop)** for distributed storage
- **Docker** for containerization and reproducible environments
- Monitoring via **NameNode / DataNode** dashboards

---

## 🛠️ Tools & Technologies

| Category | Technologies |
|--------|--------------|
| Orchestration | Docker |
| Data Collection | Python, RSS, Reddit API, Web Scraping |
| Distributed Storage | HDFS (Hadoop) |
| Distributed Processing | Apache Spark / PySpark |
| NLP & Embeddings | Sentence-BERT, UMAP |
| Topic Modeling | BERTopic, K-Means |
| Classification | Random Forest, SVM, XGBoost |
| Visualization | Streamlit, Plotly |
| Development | Jupyter Notebook, VS Code |

---

## 🧪 Demonstration

The project includes:
- HDFS folder structure screenshots
- Docker container overview
- Extracted topics visualizations
- Classification model performance results

---

## 🚀 Future Improvements

- **Real-time processing** using Apache Kafka
- **Sentiment analysis** (positive / negative / neutral)
- More advanced NLP models (LLaMA 3, GPT, BERT Large)
- **Cloud deployment** (AWS, GCP, Azure)
- **Multilingual support** (Arabic, Spanish, Amazigh)

---

## 👩‍💻 Team Members

- **Maryam Sakouti**
- **Hind Elqorachi**
- **Nadia Lahrouri**
- **Wafa Jaafar**

---

## 🎓 Supervision

- **Prof. Yassine Ait Lahcen**  
- Faculty of Sciences Agadir  
- Center of Excellence IT  
- Master of Excellence – ADIA  
- Module: Big Data

---

## 📄 License

This project is for academic and educational purposes.
