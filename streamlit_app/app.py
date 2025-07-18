
import streamlit as st
import pandas as pd
import plotly.express as px
import folium
from streamlit_folium import st_folium
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
import datetime

# Configuration de la page
st.set_page_config(page_title="Weather Data Dashboard", layout="wide")

# Connexion à Snowflake via Snowpark
session = get_active_session()


# Mise en cache des données pour optimiser les performances
@st.cache_data
def load_weather_data():
    # Récupérer les données de la table weather_data
    df = session.table("weather_data").select(
        col("city"), col("country"), col("temperature"), col("humidity"),
        col("pressure"), col("weather_main"), col("weather_description"),
        col("fetched_at"), col("wind_speed"), col("temp_min"), col("temp_max")
    ).to_pandas()
    return df


# Charger cities5000.txt depuis un stage Snowflake
@st.cache_data
def load_cities_data():
    schema = StructType([
        StructField("geonameid", StringType()),
        StructField("name", StringType()),
        StructField("asciiname", StringType()),
        StructField("alternatenames", StringType()),
        StructField("latitude", FloatType()),
        StructField("longitude", FloatType()),
        StructField("feature_class", StringType()),
        StructField("feature_code", StringType()),
        StructField("country_code", StringType()),
        StructField("cc2", StringType()),
        StructField("admin1_code", StringType()),
        StructField("admin2_code", StringType()),
        StructField("admin3_code", StringType()),
        StructField("admin4_code", StringType()),
        StructField("population", StringType()),
        StructField("elevation", StringType()),
        StructField("dem", StringType()),
        StructField("timezone", StringType()),
        StructField("modification_date", StringType())
    ])
    cities_df = session.read.format("csv").option("field_delimiter", "\t").option("skip_header", 1).schema(schema).load(
        "@weather.db_weather.weather_stage/cities5000.txt"
    ).select(col("name"), col("latitude"), col("longitude")).to_pandas()
    return cities_df


# Charger les données
df = load_weather_data()
cities_df = load_cities_data()

# Nettoyer les données (gérer les valeurs nulles)
df = df.fillna({
    "temperature": 0, "humidity": 0, "pressure": 0, "wind_speed": 0,
    "temp_min": 0, "temp_max": 0, "country": "Unknown",
    "weather_main": "Unknown", "weather_description": "Unknown"
})

# Barre latérale pour les filtres
st.sidebar.header("Filtres")
countries = df["country"].unique()
selected_countries = st.sidebar.multiselect("Pays", options=countries, default=countries[:2])
date_range = st.sidebar.date_input(
    "Plage de dates",
    [df["fetched_at"].min().date(), df["fetched_at"].max().date()],
    min_value=df["fetched_at"].min().date(),
    max_value=df["fetched_at"].max().date()
)
temp_min_filter = st.sidebar.slider("Température minimale (°C)", -50.0, 50.0, -50.0)
temp_max_filter = st.sidebar.slider("Température maximale (°C)", -50.0, 50.0, 50.0)

# Filtrer les données
filtered_df = df[
    (df["country"].isin(selected_countries)) &
    (df["fetched_at"].dt.date >= date_range[0]) &
    (df["fetched_at"].dt.date <= date_range[1]) &
    (df["temp_min"] >= temp_min_filter) &
    (df["temp_max"] <= temp_max_filter)
    ]

# Joindre avec les coordonnées pour la carte
filtered_df = filtered_df.merge(
    cities_df[["name", "latitude", "longitude"]],
    left_on="city",
    right_on="name",
    how="left"
).drop(columns=["name"], errors="ignore")

# Onglets pour différentes visualisations
tab1, tab2, tab3 = st.tabs(["Tableau", "Graphiques", "Carte"])

# Onglet 1 : Tableau interactif
with tab1:
    st.header("Données météorologiques")
    st.dataframe(
        filtered_df[["city", "country", "temperature", "temp_min", "temp_max",
                     "wind_speed", "humidity", "pressure", "weather_main",
                     "weather_description", "fetched_at"]],
        use_container_width=True
    )

# Onglet 2 : Graphiques
with tab2:
    st.header("Visualisations")

    # Graphique de température min/max par ville
    fig_temp = px.bar(
        filtered_df,
        x="city",
        y=["temp_min", "temp_max"],
        title="Température minimale et maximale par ville",
        barmode="group",
        labels={"value": "Température (°C)", "variable": "Type"}
    )
    st.plotly_chart(fig_temp, use_container_width=True)

    # Graphique de vitesse du vent
    fig_wind = px.scatter(
        filtered_df,
        x="city",
        y="wind_speed",
        color="country",
        title="Vitesse du vent par ville",
        labels={"wind_speed": "Vitesse du vent (m/s)"}
    )
    st.plotly_chart(fig_wind, use_container_width=True)

# Onglet 3 : Carte géospatiale
with tab3:
    st.header("Carte des données météorologiques")

    # Créer une carte Folium
    m = folium.Map(location=[0, 0], zoom_start=2)
    for _, row in filtered_df.iterrows():
        if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"]):
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=5,
                popup=f"{row['city']} ({row['country']}): {row['temperature']}°C",
                color="blue",
                fill=True,
                fill_color="blue"
            ).add_to(m)
    st_folium(m, width=700, height=500)

# Note sur les données
st.write("Données chargées depuis la table Snowflake 'weather_data'. Utilisez les filtres pour explorer les données.")
