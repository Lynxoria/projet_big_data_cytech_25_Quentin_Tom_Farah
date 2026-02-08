# NYC Taxi Big Data Project

Ce projet met en œuvre une architecture **Big Data complète** (type "CAC 40") pour l'analyse des données de taxis new-yorkais. Il couvre l'ensemble du cycle de vie de la donnée :
1.  **Data Lake** (MinIO)
2.  **Data Warehouse** (PostgreSQL)
3.  **ETL Pipeline** (Spark/Scala)
4.  **Machine Learning** (Scikit-Learn)
5.  **Business Intelligence** (Streamlit)

**Auteurs :** Quentin, Tom, Farah
**École :** CY Cergy Paris Université - CY Tech

---

## 1. Prérequis & Infrastructure

Assurez-vous d'avoir les outils suivants installés :
* **Docker** & Docker Compose
* **Java JDK** (8, 11 ou 17)
* **Python 3.10+**
* **uv** (Gestionnaire de paquets Python ultra-rapide)

### A. Démarrage des Services (Docker)
Nous utilisons Docker pour héberger le Data Lake (MinIO) et le Data Warehouse (Postgres).

```bash
# À la racine du projet
docker compose up -d
```

### B. Initialisation du Data Warehouse (Postgre)
Créer le schéma en étoile et insérer les données de référence en exécutant les fichiers SQL.

## 2. Ingestion & ETL (Spark/Scala)

Ouvrez le projet avec IntelliJ IDEA.

Ouvrez le fichier src/main/scala/fr/cytech/integration/Main.scala.

Lancez l'exécution via le bouton Run (Flèche verte).

## 3. Exercice 4 : Data Visualization (Streamlit)

# 1. Aller dans le dossier
```bash
cd dataviz_exercice4

# 2. Créer l'environnement virtuel et l'activer
uv venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)

# 3. Installer les dépendances
uv pip install -r requirements.txt

# 4. Lancer le Dashboard
streamlit run app.py
```
## 4. Exercice 5 : Machine Learning
```bash
# 1. Aller dans le dossier (depuis la racine)
cd machine-learning_exercice5

# 2. Créer l'environnement virtuel et l'activer
uv venv
source .venv/bin/activate

# 3. Installer les dépendances
uv pip install -r requirements.txt

# 4. Vérification Qualité du Code (Linting)
flake8 train_model.py ml_app.py

# 5. Entraînement du Modèle
# Récupère les données propres depuis MinIO -> Génère 'taxi_model.pkl'
python train_model.py
# Attendu : " SUCCÈS : RMSE < 10 respecté"

# 6. Lancer l'Interface de Prédiction
python -m streamlit run ml_app.py
```
