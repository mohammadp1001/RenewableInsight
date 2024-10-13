import os
import sys
import pandas as pd
import altair as alt
import streamlit as st
from google.cloud import bigquery
from pydantic import ValidationError
from google.oauth2 import service_account

sys.path.append("/home/mohammad/RenewableInsight/")

from src.config import Config
try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)
    st.error("Configuration error: " + str(e))
    st.stop()

try:
    credentials = service_account.Credentials.from_service_account_file('/home/mohammad/RenewableInsight/service-account-file.json')
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
except Exception as e:
    st.error("Failed to set up BigQuery client: " + str(e))
    st.stop()

st.set_page_config(
    page_title="Renewable Energy Insight",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

col1, col2, col3 = st.columns(3)

@st.cache_data(ttl=600)
def run_query(query):
    query_job = bigquery_client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return pd.DataFrame(rows)

query = """
    SELECT * 
    FROM `nimble-courier-438418-n0.renewableinsight_dataset.weather_forecast_stuttgart`
"""
try:
    results = run_query(query)
except Exception as e:
    st.error("Failed to fetch data from BigQuery: " + str(e))
    st.stop()

results['forecast_time'] = pd.to_datetime(results['forecast_time'], errors='coerce')
results = results.dropna(subset=['forecast_time'])

results['date'] = results['forecast_time'].dt.date
aggregated_results = results.groupby('date').agg({
    'wind_speed': 'mean',
    'global_irradiance': 'mean'
}).reset_index()

with col3:
    st.markdown("<h3 style='text-align: center;'>Weather Forecast for Next 3 Days</h3>", unsafe_allow_html=True)

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


query_load = """
    SELECT * 
    FROM `nimble-courier-438418-n0.renewableinsight_dataset.load`
"""
try:
    load_results = run_query(query_load)
except Exception as e:
    st.error("Failed to fetch electricity load data from BigQuery: " + str(e))
    st.stop()

load_results['date'] = pd.to_datetime(load_results['date'], errors='coerce')
load_results = load_results.dropna(subset=['date'])

with col1:
    st.markdown("<h3 style='text-align: center;'>Electricity Load Data</h3>", unsafe_allow_html=True)

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