import streamlit as st
import joblib
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

"""
=================================================================================
Projet      : NYC Taxi Big Data Architecture
Exercice    : 5 - Interface de Prédiction (Front-end ML)
Description : Interface Streamlit permettant aux utilisateurs finaux d'interroger
              le modèle de Machine Learning entraîné.
              Elle visualise également la position de la prédiction sur la courbe
              de régression pour expliquer le résultat (Explainable AI).
=================================================================================
"""

# --- CONFIGURATION ---
st.set_page_config(page_title="NYC Taxi Fare Predictor", page_icon="🤖")

st.title("🤖 Prédiction de Prix (Machine Learning)")
st.markdown("""
Cette interface permet d'interroger le modèle de **Régression Linéaire** entraîné sur les données de Q1 2023.
Entrez une distance pour estimer le coût de la course.
""")

# --- 1. CHARGEMENT DU MODÈLE ---
@st.cache_resource
def load_model():
    """
    Charge le modèle .pkl depuis le disque.
    Utilise le cache pour ne pas recharger le fichier à chaque interaction utilisateur.
    """
    try:
        model = joblib.load('taxi_model.pkl')
        return model
    except FileNotFoundError:
        st.error(" Fichier 'taxi_model.pkl' introuvable. Avez-vous lancé l'entraînement ?")
        return None

# Tentative de chargement
model = load_model()

if model:
    # --- 2. INTERFACE UTILISATEUR ---
    st.sidebar.header("Paramètres de la course")

    # Saisie de la distance
    distance = st.sidebar.slider(
        "Distance du trajet (miles)",
        min_value=0.1,
        max_value=20.0,
        value=2.5,
        step=0.1
    )

    # --- 3. PRÉDICTION ---
    if st.sidebar.button("Estimer le Prix", type="primary"):
        # Le modèle attend un tableau 2D : [[valeur]]
        features = np.array([[distance]])
        prediction = model.predict(features)[0]

        # Affichage du résultat
        st.success(f" Prix estimé : **${prediction:.2f}**")

        # --- 4. VISUALISATION CONTEXTUELLE ---
        st.subheader("Visualisation de la Prédiction")

        # On génère des données théoriques pour tracer la ligne de régression
        x_range = np.linspace(0, 25, 100)
        y_pred_line = model.predict(x_range.reshape(-1, 1))

        # Création du graphique
        fig = go.Figure()

        # A. La ligne de tendance du modèle
        fig.add_trace(go.Scatter(
            x=x_range,
            y=y_pred_line,
            mode='lines',
            name='Modèle (Tendance)',
            line=dict(color='blue')
        ))

        # B. Le point prédit (Utilisateur)
        fig.add_trace(go.Scatter(
            x=[distance],
            y=[prediction],
            mode='markers',
            name='Votre Course',
            marker=dict(color='red', size=15, symbol='star')
        ))

        fig.update_layout(
            title="Position de votre course sur la courbe de régression",
            xaxis_title="Distance (miles)",
            yaxis_title="Prix ($)",
            template="plotly_white"
        )

        st.plotly_chart(fig, use_container_width=True)

        # --- 5. EXPLICATION DU MODÈLE ---
        # On récupère les coefficients pour expliquer la logique
        coef = model.coef_[0]
        intercept = model.intercept_

        st.info(f"""
        **Comment ça marche ?**
        
        Le modèle a appris la formule suivante :
        $$ \\text{{Prix}} = {coef:.2f} \\times \\text{{Distance}} + {intercept:.2f} $$
        
        * **{intercept:.2f} $** : C'est le prix de base (prise en charge) estimé par le modèle.
        * **{coef:.2f} $/mile** : C'est le coût ajouté pour chaque mile parcouru.
        """)

else:
    st.warning("Veuillez entraîner le modèle (python train_model.py) avant de lancer cette interface.")