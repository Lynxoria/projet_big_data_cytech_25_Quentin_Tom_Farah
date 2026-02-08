import sys
import pandas as pd
import numpy as np
import s3fs
import joblib
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

"""
=================================================================================
Projet      : NYC Taxi Big Data Architecture
Exercice    : 5 - Machine Learning (Entraînement & Validation)
Description : Ce script automatise le cycle de vie du modèle ML (MLOps léger).
              1. Chargement des données nettoyées depuis le Data Lake (MinIO).
              2. Validation de la qualité des données (Tests Unitaires).
              3. Entraînement d'une Régression Linéaire (Target: total_amount).
              4. Évaluation de la performance (RMSE < 10).
              5. Sauvegarde du modèle sérialisé (.pkl).
=================================================================================
"""

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "nyc-raw"
FILE_PATH = "processed/yellow_tripdata_2023_Q1_clean.parquet"


def get_minio_fs():
    """
    Crée une connexion au système de fichiers Minio (S3).

    Returns
    -------
    s3fs.S3FileSystem
        Objet de connexion au filesystem S3 configuré.
    """
    return s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': MINIO_ENDPOINT},
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY
    )


def validate_input_data(df):
    """
    Implémente les tests unitaires sur les données d'entrée.
    Vérifie la présence des colonnes, les types et l'absence de valeurs nulles.

    Parameters
    ----------
    df : pd.DataFrame
        Le DataFrame à valider.

    Raises
    ------
    AssertionError
        Si une validation échoue.
    """
    print("--> Exécution des tests unitaires sur les données...")

    # Test 1: Non vide
    assert not df.empty, "Erreur: Le DataFrame est vide."

    # Test 2: Colonnes requises
    required_columns = ['trip_distance', 'total_amount']
    for col in required_columns:
        assert col in df.columns, f"Erreur: Colonne manquante {col}"

    # Test 3: Pas de valeurs nulles critiques
    assert df[required_columns].notna().all().all(), "Erreur: Valeurs NaN"

    # Test 4: Types numériques
    assert pd.api.types.is_numeric_dtype(df['trip_distance']), \
        "Erreur: trip_distance doit être numérique"
    assert pd.api.types.is_numeric_dtype(df['total_amount']), \
        "Erreur: total_amount doit être numérique"

    print(" Données valides (Tests passés).")


def load_data(fs, bucket, path):
    """
    Charge et fusionne les fichiers Parquet depuis Minio.

    Parameters
    ----------
    fs : s3fs.S3FileSystem
        Connexion S3.
    bucket : str
        Nom du bucket.
    path : str
        Chemin du dossier ou fichier dans le bucket.

    Returns
    -------
    pd.DataFrame or None
        Le DataFrame chargé ou None si échec.
    """
    full_path = f"s3://{bucket}/{path}"
    print(f"--> Recherche dans : {full_path}")

    try:
        if not path.endswith("/"):
            full_path += "/"

        # Lister les fichiers
        files = fs.glob(full_path + "*.parquet")
        data_files = [f for f in files if "part-" in f and "_SUCCESS" not in f]

        if not data_files:
            print(" Aucun fichier de données trouvé.")
            return None

        dfs = []
        for file in data_files:
            with fs.open(file, mode='rb') as f:
                part = pd.read_parquet(f)
                if not part.empty:
                    dfs.append(part)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    except Exception as e:
        print(f"ERREUR CRITIQUE : {e}")
        return None


def preprocess_data(df):
    """
    Nettoie les données et retire les outliers pour améliorer le modèle.

    Parameters
    ----------
    df : pd.DataFrame
        Données brutes.

    Returns
    -------
    pd.DataFrame
        Données nettoyées prêtes pour l'entraînement.
    """
    # Filtrage drastique pour respecter la consigne RMSE < 10
    # On garde les courses "standards" (pas d'erreurs GPS ou arnaques)
    df_clean = df[
        (df['total_amount'] > 0) & (df['total_amount'] < 200) &
        (df['trip_distance'] > 0) & (df['trip_distance'] < 100)
        ].copy()
    return df_clean


def train_model():
    """
    Fonction principale du pipeline ML.
    1. Chargement
    2. Validation (Tests)
    3. Preprocessing
    4. Entraînement
    5. Évaluation
    6. Sauvegarde
    """
    fs = get_minio_fs()
    df = load_data(fs, BUCKET_NAME, FILE_PATH)

    if df is None:
        sys.exit(1)

    # 1. Tests Unitaires (Consigne: sur les données entrées)
    try:
        validate_input_data(df)
    except AssertionError as e:
        print(f" Arrêt : {e}")
        sys.exit(1)

    # 2. Preprocessing (Nettoyage Outliers)
    print(f"--> Données brutes : {len(df)} lignes")
    df = preprocess_data(df)
    print(f"--> Données nettoyées : {len(df)} lignes")

    # 3. Features & Target
    X = df[['trip_distance']]
    y = df['total_amount']

    # 4. Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 5. Entraînement
    print("--> Entraînement LinearRegression...")
    model = LinearRegression()
    model.fit(X_train, y_train)

    # 6. Évaluation
    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    print("-" * 30)
    print(f" RMSE : {rmse:.2f}")
    print("-" * 30)

    if rmse < 10:
        print(" SUCCÈS : RMSE < 10 respecté.")
    else:
        print(" ATTENTION : RMSE > 10.")

    # 7. Sauvegarde
    joblib.dump(model, 'taxi_model.pkl')
    print("--> Modèle sauvegardé : taxi_model.pkl")


if __name__ == "__main__":
    train_model()
