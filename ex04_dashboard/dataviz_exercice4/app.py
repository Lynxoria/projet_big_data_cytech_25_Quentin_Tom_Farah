import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# 1. Configuration de la page
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")
st.title("🚖 NYC Taxi Data Warehouse Dashboard")

# 2. Connexion à la Base de Données (Postgres)
# On utilise SQLAlchemy pour faciliter la lecture avec Pandas
@st.cache_data
def load_data():
    # Format: postgresql://user:password@host:port/database
    db_connection_str = 'postgresql://postgres:postgres@localhost:5432/postgres'
    db_connection = create_engine(db_connection_str)

    query = """
    SELECT 
        tpep_pickup_datetime as date,
        passenger_count,
        trip_distance,
        total_amount,
        payment_type_id
    FROM fact_trips
    """

    try:
        df = pd.read_sql(query, db_connection)
        return df
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return pd.DataFrame()

# Chargement des données
df = load_data()

if not df.empty:
    # 3. KPIs (Indicateurs clés)
    st.header("Vue d'ensemble")
    col1, col2, col3 = st.columns(3)

    total_revenue = df['total_amount'].sum()
    avg_distance = df['trip_distance'].mean()
    total_trips = len(df)

    col1.metric("Chiffre d'Affaires Total", f"${total_revenue:,.2f}")
    col2.metric("Distance Moyenne", f"{avg_distance:.2f} miles")
    col3.metric("Nombre de Courses", f"{total_trips}")

    st.markdown("---")

    # 4. Graphiques
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.subheader("Distribution des Prix")
        fig_hist = px.histogram(df, x="total_amount", nbins=50, title="Répartition du prix des courses")
        st.plotly_chart(fig_hist, use_container_width=True)

    with col_g2:
        st.subheader("Relation Distance vs Prix")
        # On filtre les valeurs extrêmes pour la lisibilité
        fig_scatter = px.scatter(
            df[df['trip_distance'] < 20],
            x="trip_distance",
            y="total_amount",
            title="Prix selon la distance",
            opacity=0.5
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # 5. Données Brutes
    st.subheader("Aperçu des Données (Data Mart)")
    st.dataframe(df.head())

else:
    st.warning("Aucune donnée trouvée. Vérifiez que le conteneur Postgres tourne et que l'ingestion Spark a fonctionné.")

st.markdown("---")
st.header("Analyses Approfondies")

col3, col4 = st.columns(2)

with col3:
    st.subheader("Heures de Pointe")
    # On extrait l'heure de la date
    df['hour'] = pd.to_datetime(df['date']).dt.hour
    hourly_counts = df.groupby('hour').size().reset_index(name='counts')

    fig_time = px.bar(
        hourly_counts,
        x='hour',
        y='counts',
        title="Nombre de courses par heure de la journée",
        labels={'hour': 'Heure', 'counts': 'Nombre de courses'}
    )
    st.plotly_chart(fig_time, use_container_width=True)

with col4:
    st.subheader("Types de Paiement")
    # On mappe les IDs (1=Credit Card, 2=Cash, etc.) pour que ce soit lisible
    payment_map = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 0: 'Unknown'}
    df['payment_name'] = df['payment_type_id'].map(payment_map).fillna("Autre")

    payment_counts = df.groupby('payment_name').size().reset_index(name='counts')

    fig_pie = px.pie(
        payment_counts,
        values='counts',
        names='payment_name',
        title="Répartition des moyens de paiement",
        hole=0.4 # Pour faire un donut chart
    )
    st.plotly_chart(fig_pie, use_container_width=True)