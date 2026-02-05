import streamlit as st
import joblib
import pandas as pd
import numpy as np

# Configuration
st.set_page_config(page_title="Taxi Price Predictor", page_icon="🚖")

st.title("🚖 Prédiction du Prix de la Course")
st.write("Ce modèle utilise une régression linéaire entraînée sur les données de Janvier 2023.")

# Chargement du modèle
try:
    model = joblib.load('taxi_model.pkl')
    st.success("Modèle chargé avec succès !")
except FileNotFoundError:
    st.error("Erreur : Le fichier 'taxi_model.pkl' est introuvable. Lancez d'abord train_model.py.")
    st.stop()

# Formulaire utilisateur
distance = st.number_input(
    "Distance de la course (en miles)",
    min_value=0.1,
    max_value=100.0,
    value=2.5,
    step=0.1
)

# Bouton de prédiction
if st.button("Estimer le Prix"):
    # Création du DataFrame pour la prédiction (même format que l'entraînement)
    input_data = pd.DataFrame([[distance]], columns=['trip_distance'])

    # Prédiction
    prediction = model.predict(input_data)[0]

    # Affichage
    st.metric(label="Prix Estimé", value=f"${prediction:.2f}")

    # Petit détail contextuel
    if prediction < 0:
        st.warning("Le modèle prédit un prix négatif (c'est une limite de la régression linéaire sur les très courtes distances !)")