import os
import sys
import pandas as pd
import altair as alt
import streamlit as st

from google.cloud import bigquery
from pydantic import ValidationError
from google.oauth2 import service_account

# Load configuration
path_to_append = '/home/mohammad/RenewableInsight'
if path_to_append:
    sys.path.append(path_to_append)

from src.config import Config

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)
    st.error("Configuration error: " + str(e))
    st.stop()

# Set up BigQuery client
try:
    credentials = service_account.Credentials.from_service_account_file('/home/mohammad/RenewableInsight/service-account-file.json')
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
except Exception as e:
    st.error("Failed to set up BigQuery client: " + str(e))
    st.stop()

# Set Streamlit page config
st.set_page_config(
    page_title="Renewable Energy Insight",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded")

# Enable dark theme for Altair charts
alt.themes.enable("dark")

# Create columns for layout
col1, col2, col3 = st.columns(3)

# Fetch weather forecast data from BigQuery using caching
@st.cache_data(ttl=600)
def run_query(query):
    query_job = bigquery_client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return pd.DataFrame(rows)

# Run the query
query = """
    SELECT * 
    FROM `woven-rush-429121-f0.renewableinsight_dataset.weather_forecast_stuttgart`
"""
try:
    results = run_query(query)
except Exception as e:
    st.error("Failed to fetch data from BigQuery: " + str(e))
    st.stop()

# Ensure forecast_time is in datetime format
results['forecast_time'] = pd.to_datetime(results['forecast_time'], errors='coerce')
results = results.dropna(subset=['forecast_time'])

# Aggregate data by day
results['date'] = results['forecast_time'].dt.date
aggregated_results = results.groupby('date').agg({
    'wind_speed': 'mean',
    'global_irradiance': 'mean'
}).reset_index()


# Right Column: Weather Forecast Information
with col3:
    st.markdown("<h3 style='text-align: center;'>Weather Forecast for Next 3 Days</h3>", unsafe_allow_html=True)

    # Wind Speed Point Chart
    wind_speed_df = pd.DataFrame({
        'Date': aggregated_results['date'],
        'Average Wind Speed (km/h)': aggregated_results['wind_speed']
    }).dropna(subset=['Average Wind Speed (km/h)'])

    wind_speed_chart = alt.Chart(wind_speed_df).mark_point(filled=True, size=100).encode(
        x=alt.X('Date:T', axis=alt.Axis(format='%b %d, %Y', title='Date', labelPadding=10)),
        y=alt.Y('Average Wind Speed (km/h):Q', axis=alt.Axis(title='Average Wind Speed (km/h)', labelPadding=10)),
        color=alt.value('blue')
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_view(
        strokeWidth=0
    ).interactive()

    st.altair_chart(wind_speed_chart, use_container_width=True)

    # Global Irradiance Bar Chart
    irradiance_df = pd.DataFrame({
        'Date': aggregated_results['date'],
        'Average Global Irradiance (W/m²)': aggregated_results['global_irradiance']
    }).dropna(subset=['Average Global Irradiance (W/m²)'])

    bar_chart = alt.Chart(irradiance_df).mark_bar().encode(
        x=alt.X('Date:T', axis=alt.Axis(format='%b %d, %Y', title='Date', labelPadding=10)),
        y='Average Global Irradiance (W/m²):Q',
        color=alt.value('orange')
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_view(
        strokeWidth=0
    ).interactive()

    st.altair_chart(bar_chart, use_container_width=True)


# Fetch electricity load data from BigQuery using caching
query_load = """
    SELECT * 
    FROM `woven-rush-429121-f0.renewableinsight_dataset.load`
"""
try:
    load_results = run_query(query_load)
except Exception as e:
    st.error("Failed to fetch electricity load data from BigQuery: " + str(e))
    st.stop()

# Ensure date is in datetime format
load_results['date'] = pd.to_datetime(load_results['date'], errors='coerce')
load_results = load_results.dropna(subset=['date'])

# Left Column: Electricity Load Information
with col1:
    st.markdown("<h3 style='text-align: center;'>Electricity Load Data</h3>", unsafe_allow_html=True)

    # Total Electricity Load (Last 5 Days)
    load_5_days = load_results.groupby('date').agg({'load': 'sum'}).reset_index().tail(5)
    total_load_chart = alt.Chart(load_5_days).mark_bar().encode(
        x=alt.X('date:T', axis=alt.Axis(format='%b %d, %Y', title='Date', labelPadding=10)),
        y=alt.Y('load:Q', axis=alt.Axis(title='Total Load (MW)', labelPadding=10)),
        color=alt.value('green')
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_view(
        strokeWidth=0
    ).interactive()

    st.altair_chart(total_load_chart, use_container_width=True)

    # Load by Hour (Last 24 Hours)
    load_results['datetime'] = pd.to_datetime(load_results[['year', 'month', 'day', 'hour', 'minute']], errors='coerce')
    load_24_hours = load_results.sort_values(by='datetime').tail(24)
    load_24_hours_chart = alt.Chart(load_24_hours).mark_line(point=True).encode(
        x=alt.X('datetime:T', axis=alt.Axis(format='%b %d, %Y %H:%M', title='Time', labelPadding=10)),
        y=alt.Y('load:Q', axis=alt.Axis(title='Load (MW)', labelPadding=10)),
        color=alt.value('purple')
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_view(
        strokeWidth=0
    ).interactive()

    st.altair_chart(load_24_hours_chart, use_container_width=True)