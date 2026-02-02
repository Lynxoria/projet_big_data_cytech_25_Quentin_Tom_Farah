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
    LIMIT 10000 -- On limite pour l'exemple, retirez la limite en prod
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