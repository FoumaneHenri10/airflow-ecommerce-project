# Pipeline E-Commerce Industrialisé & Big Data (Airflow, MongoDB & Docker)

## 📌 Contexte du Projet
Projet de Mastère 2 Data Engineering visant à industrialiser le traitement et l'analyse de données e-commerce à grande échelle (basé sur l'**Olist Brazilian E-Commerce Dataset**). Le pipeline automatise l'ingestion, le contrôle qualité, la transformation et le stockage persistant des indicateurs métiers.

## 🏗️ Architecture Technique
- **Orchestration :** Apache Airflow (v2.9.1)
- **Stockage NoSQL :** MongoDB (Base : `ecommerce_analytics`, Collection : `sales_metrics`)
- **Containerisation :** Docker & Docker Compose (Services isolés pour Airflow, le Scheduler et MongoDB)
- **Gestion de version :** Git (Modèle à deux branches : `main` et `dev`)

## 🚀 Fonctionnalités du DAG (`ecommerce_sales_pipeline`)
1. **FileSensor :** Détection automatisée de la présence du fichier source CSV.
2. **Contrôle Qualité** : Vérification de l'intégrité des données, filtrage des anomalies selon les règles de gestion métier, et isolation automatique des lignes rejetées dans un fichier d'erreurs dédié (errors.csv).
3. **Indicateurs Métier :** Calcul du nombre total de commandes, du chiffre d'affaires global, du panier moyen, du top 10 des produits et de la ventilation géographique par région.
4. **Historisation NoSQL :** Sérialisation et stockage des métriques sous forme de documents JSON historisés dans MongoDB.

## 🛠️ Lancement Rapide du Projet
Pour déployer l'ensemble de l'infrastructure en local via Docker Compose :
```bash
docker-compose up -d --build