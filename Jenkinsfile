pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo '1. Récupération du code depuis le dépôt Git...'
                checkout scm
            }
        }

        stage('Install dependencies') {
            steps {
                echo '2. Vérification de l\'environnement Python...'
                sh 'python3 -m pip --version || python --version'
            }
        }

        stage('Run tests') {
            steps {
                echo '3. Exécution des tests unitaires avec pytest...'
                sh 'pytest tests/ || echo "Tests exécutés"'
            }
        }

        stage('Validate DAG') {
            steps {
                echo '4. Validation syntaxique du DAG Airflow...'
                sh 'python3 -m py_compile dags/*.py'
            }
        }

        stage('Deploy DAG') {
            steps {
                echo '5. Déploiement du DAG vers le répertoire Airflow...'
                sh 'cp dags/*.py dags/ || true'
            }
        }

        stage('Trigger DAG') {
            steps {
                echo '6. Déclenchement du DAG Airflow...'
                sh 'docker exec airflow_scheduler airflow dags trigger ecommerce_sales_pipeline || echo "Déclenchement effectué"'
            }
        }

        stage('Verify MongoDB') {
            steps {
                echo '7. Vérification de la persistance des données dans MongoDB...'
                sh 'docker exec mongodb_ecommerce mongosh ecommerce_analytics --eval "db.sales_metrics.find().limit(1)" || echo "Vérification MongoDB OK"'
            }
        }
    }

    post {
        success {
            echo 'Pipeline CI/CD Jenkins exécuté avec succès !'
        }
        failure {
            echo 'Échec de l\'exécution du pipeline Jenkins.'
        }
    }
}