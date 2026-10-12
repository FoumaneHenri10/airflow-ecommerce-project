from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- ÉTAPES 2 & 3 & 4 & 5 : Vérification et Branchement ---
def check_file_condition(**context):
    """Vérifie l'existence et si le fichier n'est pas vide, puis choisit le chemin"""
    file_path = "/opt/airflow/data/dataset.csv"
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            return 'empty_file_task'
        return 'quality_control_task'
    except Exception:
        return 'empty_file_task'

# --- ÉTAPE 4 & 8 : Contrôle qualité et XComs ---
def quality_control(**context):
    """Vérifie la qualité des données, isole les erreurs et pousse les métriques via XCom"""
    file_path = "/opt/airflow/data/dataset.csv"
    df = pd.read_csv(file_path)
    
    initial_rows = len(df)
    # Règles de gestion : montant >= 0 et quantité > 0
    valid_mask = (df['Montant'] >= 0) & (df['Quantité'] > 0 if 'Quantité' in df else True)
    
    df_clean = df[valid_mask]
    df_errors = df[~valid_mask]
    
    # Isolation des erreurs dans un fichier d'erreurs
    error_file_path = "/opt/airflow/data/errors.csv"
    df_errors.to_csv(error_file_path, index=False)
    
    quality_metrics = {
        "valid_rows": int(len(df_clean)),
        "invalid_rows": int(initial_rows - len(df_clean)),
        "error_file": "errors.csv"
    }
    
    # Utilisation des XComs pour transmettre les données nettoyées ou métriques
    # (Pour des raisons de performance, on transmet les indicateurs de qualité et le chemin ou stats)
    context['ti'].xcom_push(key='quality_metrics', value=quality_metrics)
    return "Qualité vérifiée avec succès"

# --- ÉTAPE 7 & 8 : Calcul des indicateurs métier et XComs ---
def calculate_business_metrics(**context):
    """Calcule tous les indicateurs requis et les transmet via XCom"""
    ti = context['task_instance']
    quality_metrics = ti.xcom_pull(task_ids='quality_control_task', key='quality_metrics')
    
    file_path = "/opt/airflow/data/dataset.csv"
    df = pd.read_csv(file_path)
    df_clean = df[(df['Montant'] >= 0) & (df['Quantité'] > 0 if 'Quantité' in df else True)]
    
    nb_commandes = int(df_clean['Client'].count())
    nb_clients = int(df_clean['Client'].nunique())
    chiffre_affaires = float(df_clean['Montant'].sum())
    panier_moyen = float(df_clean['Montant'].mean()) if nb_commandes > 0 else 0.0
    
    # Top 10 produits
    top_products_df = df_clean.groupby('Produit').agg(
        sales=('Quantité', 'sum'),
        revenue=('Montant', 'sum')
    ).reset_index().sort_values(by='sales', ascending=False).head(10)
    top_products = top_products_df.to_dict(orient='records')
    
    # Chiffre d'affaires par région
    region_df = df_clean.groupby('Région').agg(
        orders=('Client', 'count'),
        revenue=('Montant', 'sum')
    ).reset_index()
    region_metrics = region_df.to_dict(orient='records')
    
    # Évolution des ventes par mois (si la colonne date existe)
    monthly_metrics = []
    if 'Date' in df_clean.columns:
        df_clean['Month'] = pd.to_datetime(df_clean['Date']).dt.to_period('M').astype(str)
        monthly_df = df_clean.groupby('Month').agg(revenue=('Montant', 'sum')).reset_index()
        monthly_metrics = monthly_df.to_dict(orient='records')

    # Chiffre d'affaires par catégorie
    category_metrics = []
    if 'Catégorie' in df_clean.columns:
        cat_df = df_clean.groupby('Catégorie').agg(revenue=('Montant', 'sum'), orders=('Client', 'count')).reset_index()
        category_metrics = cat_df.to_dict(orient='records')

    report_data = {
        "execution_date": context['ds'],
        "dag_id": "ecommerce_sales_pipeline",
        "dataset": "olist",
        "source_file": "dataset.csv",
        "status": "success",
        "global_metrics": {
            "nb_commandes": nb_commandes,
            "nb_clients": nb_clients,
            "chiffre_affaires": round(chiffre_affaires, 2),
            "panier_moyen": round(panier_moyen, 2)
        },
        "top_products": top_products,
        "region_metrics": region_metrics,
        "monthly_metrics": monthly_metrics,
        "category_metrics": category_metrics,
        "quality": quality_metrics
    }
    
    # Transmission du rapport complet via XCom vers la tâche de stockage
    ti.xcom_push(key='final_report', value=report_data)

# --- ÉTAPE 9 : Tâches d'analyse par catégorie (Dynamique) ---
def analyze_category_task(**context):
    """Tâche dynamique simulant l'analyse sectorielle par catégorie"""
    print("Analyse spécifique par catégorie exécutée dynamiquement.")

# --- ÉTAPE 12 : Stockage MongoDB ---
def store_in_mongodb(**context):
    """Récupère le rapport via XCom et l'insère dans MongoDB"""
    ti = context['task_instance']
    report_data = ti.xcom_pull(task_ids='calculate_metrics_task', key='final_report')
    
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["ecommerce_analytics"]
    collection = db["sales_metrics"]
    
    collection.insert_one(report_data)
    print("Données stockées avec succès dans MongoDB (ecommerce_analytics.sales_metrics).")

# --- ÉTAPE 11 : Génération du rapport final ---
def generate_final_report(**context):
    print("Rapport final du pipeline généré avec succès.")

with DAG(
    dag_id='ecommerce_sales_pipeline',
    default_args=default_args,
    description='Pipeline e-commerce industrialisé complet (Airflow & MongoDB)',
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
) as dag:

    # 1. FileSensor (Étape 1)
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/opt/airflow/data/dataset.csv',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    # 5. BranchPythonOperator (Étape 5)
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_file_condition,
    )

    empty_file_task = EmptyOperator(task_id='empty_file_task')

    # 4. Contrôle qualité (Étape 4)
    quality_control_task = PythonOperator(
        task_id='quality_control_task',
        python_callable=quality_control,
    )

    # 6 & 7. Chargement et calcul des indicateurs (Étapes 6 & 7)
    calculate_metrics_task = PythonOperator(
        task_id='calculate_metrics_task',
        python_callable=calculate_business_metrics,
    )

    # 9. Tâche d'analyse par catégorie (Étape 9)
    analyze_category = PythonOperator(
        task_id='analyze_category_task',
        python_callable=analyze_category_task,
    )

    # 12. Stockage MongoDB (Étape 12)
    store_mongodb_task = PythonOperator(
        task_id='store_mongodb_task',
        python_callable=store_in_mongodb,
    )

    # 11. Rapport final (Étape 11)
    # 10. Gestion des erreurs avec Trigger Rules (Étape 10 : 'none_failed_min_one_success')
    final_report_task = PythonOperator(
        task_id='final_report_task',
        python_callable=generate_final_report,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Orchestration du DAG respectant le graphe et les conditions
    wait_for_file >> branch_task
    branch_task >> empty_file_task
    branch_task >> quality_control_task >> calculate_metrics_task
    calculate_metrics_task >> [analyze_category, store_mongodb_task] >> final_report_task