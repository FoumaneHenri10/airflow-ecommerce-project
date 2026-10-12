import os
import sys

# Ajout du chemin vers les DAGs pour s'assurer de leur importabilité
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

def test_dags_folder_exists():
    """Vérifie que le dossier des DAGs Airflow est présent."""
    dags_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags'))
    assert os.path.exists(dags_path), "Le dossier dags est introuvable."

def test_python_files_in_dags():
    """Vérifie qu'il y a bien au moins un script de DAG valide."""
    dags_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags'))
    dag_files = [f for f in os.listdir(dags_path) if f.endswith('.py')]
    assert len(dag_files) > 0, "Aucun fichier DAG Python n'a été détecté."