import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# 1. Configuration de la page
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")
st.title("🚖 NYC Taxi Data Warehouse Dashboard")

# 2. Connexion à la Base de Données (Postgres)
@st.cache_resource
def get_db_connection():
    # Format: postgresql+psycopg2://user:password@host:port/database
    return create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

engine = get_db_connection()

# --- CHARGEMENT OPTIMISÉ (SQL) ---
# On ne charge plus TOUT le dataframe d'un coup, on fait des requêtes spécifiques

@st.cache_data
def load_kpi_data():
    query = """
            SELECT
                SUM(total_amount) as total_revenue,
                AVG(trip_distance) as avg_distance,
                COUNT(*) as total_trips
            FROM fact_trips \
            """
    return pd.read_sql(query, engine)

@st.cache_data
def load_histogram_data():
    # On échantillonne pour l'histogramme aussi, sinon c'est trop lourd
    query = "SELECT total_amount FROM fact_trips WHERE total_amount BETWEEN 0 AND 200 ORDER BY RANDOM() LIMIT 100000"
    return pd.read_sql(query, engine)

@st.cache_data
def load_scatter_sample():
    # --- MODIFICATION ICI : ÉCHANTILLONNAGE ---
    # On prend 10 000 points au hasard pour le scatter plot Distance vs Prix
    query = """
            SELECT trip_distance, total_amount
            FROM fact_trips
            WHERE trip_distance > 0 AND trip_distance < 100
              AND total_amount > 0 AND total_amount < 200
            ORDER BY RANDOM()
            LIMIT 10000 \
            """
    return pd.read_sql(query, engine)

@st.cache_data
def load_hourly_data():
    # Agrégation SQL pour les heures de pointe
    query = """
            SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) as hour, COUNT(*) as counts
            FROM fact_trips
            GROUP BY hour
            ORDER BY hour \
            """
    return pd.read_sql(query, engine)

@st.cache_data
def load_payment_data():
    # Agrégation SQL pour les paiements
    query = """
            SELECT payment_type_id, COUNT(*) as counts
            FROM fact_trips
            GROUP BY payment_type_id \
            """
    return pd.read_sql(query, engine)

@st.cache_data
def load_preview_data():
    return pd.read_sql("SELECT * FROM fact_trips LIMIT 5", engine)


# 3. KPIs (Indicateurs clés)
try:
    kpi_df = load_kpi_data()

    st.header("Vue d'ensemble")
    col1, col2, col3 = st.columns(3)

    col1.metric("Chiffre d'Affaires Total", f"${kpi_df['total_revenue'][0]:,.2f}")
    col2.metric("Distance Moyenne", f"{kpi_df['avg_distance'][0]:.2f} miles")
    col3.metric("Nombre de Courses", f"{kpi_df['total_trips'][0]:,.0f}")

    st.markdown("---")

    # 4. Graphiques
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.subheader("Distribution des Prix")
        df_hist = load_histogram_data()
        fig_hist = px.histogram(df_hist, x="total_amount", nbins=50, title="Répartition du prix des courses (Échantillon)")
        st.plotly_chart(fig_hist, use_container_width=True)

    with col_g2:
        st.subheader("Relation Distance vs Prix")
        # Utilisation de l'échantillon chargé spécifiquement
        df_scatter = load_scatter_sample()
        fig_scatter = px.scatter(
            df_scatter,
            x="trip_distance",
            y="total_amount",
            title="Prix selon la distance (Basé sur 10k points)",
            opacity=0.5
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # 5. Données Brutes
    st.subheader("Aperçu des Données (Data Mart)")
    st.dataframe(load_preview_data())

    st.markdown("---")
    st.header("Analyses Approfondies")

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Heures de Pointe")
        df_hourly = load_hourly_data()
        fig_time = px.bar(
            df_hourly,
            x='hour',
            y='counts',
            title="Nombre de courses par heure de la journée",
            labels={'hour': 'Heure', 'counts': 'Nombre de courses'}
        )
        st.plotly_chart(fig_time, use_container_width=True)

    with col4:
        st.subheader("Types de Paiement")
        df_payment = load_payment_data()
        payment_map = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 0: 'Unknown', 5: 'Unknown'}
        df_payment['payment_name'] = df_payment['payment_type_id'].map(payment_map).fillna("Autre")

        fig_pie = px.pie(
            df_payment,
            values='counts',
            names='payment_name',
            title="Répartition des moyens de paiement",
            hole=0.4
        )
        st.plotly_chart(fig_pie, use_container_width=True)

except Exception as e:
    st.error(f"Erreur lors du chargement des données : {e}")
    st.warning("Vérifiez que Postgres tourne et contient des données.")