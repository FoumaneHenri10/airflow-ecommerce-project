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
2. **Contrôle Qualité :** Vérification de l'intégrité des lignes, gestion des valeurs nulles et isolation des anomalies dans un fichier d'erreurs (`errors.csv`)->(ou le mécanisme de gestion des erreurs), qui n'apparaîtra pas toujours car il est géré dynamiquement par le script Python d'ingestion/transformation (probablement dans fusion_dataset.py ou du DAG) au moment où les données sont traitées : si le jeu de données propre ne contient aucune ligne corrompue critique lors du run, le fichier n'est pas forcément écrit sur le disque, ou il est généré à la volée par le script dans le conteneur.
3. **Indicateurs Métier :** Calcul du nombre total de commandes, du chiffre d'affaires global, du panier moyen, du top 10 des produits et de la ventilation géographique par région.
4. **Historisation NoSQL :** Sérialisation et stockage des métriques sous forme de documents JSON historisés dans MongoDB.

## 🛠️ Lancement Rapide du Projet
Pour déployer l'ensemble de l'infrastructure en local via Docker Compose :
```bash
docker-compose up -d --build