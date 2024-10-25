import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery
from pydantic import ValidationError
from google.oauth2 import service_account


try:
    credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
    )
    bigquery_client = bigquery.Client(credentials=credentials, project=st.secrets['PROJECT_ID'])
except Exception as e:
    st.error("Failed to set up BigQuery client: " + str(e))
    st.stop()

st.set_page_config(
    page_title="Renewable Energy Insights", page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded")

current_dir = os.path.dirname(__file__)
st.image(current_dir, use_column_width=False, width=150)
st.markdown("<h1 style='color: green; font-size: 28px;'>Renewable Energy Insights for Baden Württemberg</h1>", unsafe_allow_html=True)


@st.cache_data(ttl=600)
def run_query(query):
    query_job = bigquery_client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return pd.DataFrame(rows)

tab1, tab2 = st.tabs(["Electricity Data", "Weather Data"])

with tab2:
    query = """
        SELECT * 
        FROM `nimble-courier-438418-n0.renewableinsight_dataset.weather_forecast_stuttgart`
    """
    try:
        results = run_query(query)
    except Exception as e:
        st.error("Failed to fetch data from BigQuery: " + str(e))
        st.stop()

    results['forecast_time'] = pd.to_datetime(results['forecast_time'])

    start_date = pd.to_datetime(pd.Timestamp('today').normalize())
    end_date = start_date + pd.Timedelta(days=3)
    filtered_df = results[(results['forecast_time'] >= start_date) & (results['forecast_time'] < end_date)]

    st.markdown("The following graphs show weather trends for the next three days at Flughafen Stuttgart, including wind speed, global irradiance, and sunshine duration.")

    col1, col2, col3 = st.columns(3, gap="small")

    with col1:
       
        aggregated_df = filtered_df.groupby(filtered_df['forecast_time']).mean().reset_index()
        aggregated_df['forecast_time'] = aggregated_df['forecast_time'].apply(lambda x: x.strftime('%b %d'))
        aggregated_df['forecast_time'] = aggregated_df['forecast_time'].astype(str)

       
        wind_chart = px.bar(aggregated_df, x='forecast_time', y='wind_speed',
                            title='Wind Speed (Flughafen Stuttgart)',
                            template='plotly_white',
                            text=aggregated_df['wind_speed'].apply(lambda x: f'{x:.2f}' if pd.notna(x) else ''))

        wind_chart.update_layout(title={'x': 0.5}, xaxis_title='Date', yaxis_title='Wind Speed (m/s)',
                                 autosize=False, width=400, height=600, bargap=0.1)
        wind_chart.update_traces(marker_color='blue', textposition='outside', textfont=dict(size=18))
        st.plotly_chart(wind_chart, use_container_width=False, key='wind_speed_chart')

    with col2:
        
        global_irradiance_chart = px.bar(aggregated_df, x='forecast_time', y='global_irradiance',
                                         title='Global Irradiance (Flughafen Stuttgart)',
                                         template='plotly_white',
                                         text=aggregated_df['global_irradiance'].apply(lambda x: f'{x:.2f}' if pd.notna(x) else ''))

        global_irradiance_chart.update_layout(title={'x': 0.5}, xaxis_title='Date', yaxis_title='Global Irradiance (W/m^2)',
                                              autosize=False, width=400, height=600, bargap=0.1)
        global_irradiance_chart.update_traces(marker_color='blue', textposition='outside', textfont=dict(size=18))
        st.plotly_chart(global_irradiance_chart, use_container_width=False, key='global_irradiance_chart')

    with col3:
        
        sunshine_chart = px.bar(aggregated_df, x='forecast_time', y='sunshine_dur',
                                title='Sunshine Duration (Flughafen Stuttgart)',
                                template='plotly_white',
                                text=aggregated_df['sunshine_dur'].apply(lambda x: f'{x:.2f}' if pd.notna(x) else ''))

        sunshine_chart.update_layout(title={'x': 0.5}, xaxis_title='Date', yaxis_title='Sunshine Duration (minutes)',
                                     autosize=False, width=400, height=600, bargap=0.1)
        sunshine_chart.update_traces(marker_color='blue', textposition='outside', textfont=dict(size=18))
        st.plotly_chart(sunshine_chart, use_container_width=False, key='sunshine_duration_chart')

with tab1:
    st.markdown("The following graphs display electricity data, including generation by types and total load in Baden Württemberg.")
    
    col1, col2 = st.columns(2, gap="small")

    with col2:
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

        load_5_days = load_results.groupby('date').agg({'load': 'sum'}).reset_index()

        load_5_days['date'] = load_5_days['date'].dt.strftime('%b %d')

        load_5_days_fig = px.bar(load_5_days, x='date', y='load',
                                 labels={'date': 'Date', 'load': 'Total Load (MW)'},
                                 template='plotly_dark')
        load_5_days_fig.update_layout(
            title="Total Load for Last 5 Days (Baden Württemberg)",
            autosize=False,
            width=400,
            height=600,
            margin=dict(
                l=50,
                r=50,
                b=100,
                t=100,
                pad=4
            ),
        )
        load_5_days_fig.update_traces(marker_color='blue', width=0.4)
        st.plotly_chart(load_5_days_fig, use_container_width=False, key='load_5_days_fig')

        query_load = """
            SELECT * FROM `nimble-courier-438418-n0.renewableinsight_dataset.gas` 
        """
        try:
            gas_data = run_query(query_load)
        except Exception as e:
            st.error("Failed to fetch electricity load data from BigQuery: " + str(e))
            st.stop()

        gas_data['date'] = pd.to_datetime(gas_data['date']).dt.strftime('%b %d')
        gas_data = gas_data.groupby('date', as_index=False).agg({'open_price': 'mean', 'close_price': 'mean'})
        gas_data = gas_data.tail(3)

        gas_data['average_price'] = gas_data[['open_price', 'close_price']].mean(axis=1)
        gas_data['price_change_pct'] = gas_data['average_price'].pct_change() * 100

        gas_chart = px.bar(gas_data, x='date', y='average_price',
                           title='Average Gas Prices',
                           template='plotly_white',
                           text=gas_data['price_change_pct'].apply(lambda x: f'{x:.2f}%' if pd.notna(x) else ''),
                           )

        gas_chart.update_layout(title={'x': 0.5}, yaxis_title='Price ($)',
                                autosize=False,
                                width=400,
                                height=600,)
        gas_chart.update_traces(marker_color='red', width=0.4, textposition='outside', textfont=dict(size=18))
        st.plotly_chart(gas_chart, use_container_width=False, key='gas_price_chart')

    with col1:
        query = """
            SELECT * FROM 
            `nimble-courier-438418-n0.renewableinsight_dataset.actual_generation` 
        """
        try:
            df = run_query(query)
        except Exception as e:
            st.error("Failed to fetch data from BigQuery: " + str(e))
            st.stop() 
        numeric_columns = [
            "biomass_actual_aggregated",
            "fossil_brown_coal_lignite_actual_aggregated",
            "fossil_gas_actual_aggregated",
            "fossil_hard_coal_actual_aggregated",
            "fossil_oil_actual_aggregated",
            "geothermal_actual_aggregated",
            "hydro_pumped_storage_actual_aggregated",
            "hydro_run-of-river_and_poundage_actual_aggregated",
            "solar_actual_consumption",
            "wind_offshore_actual_aggregated",
            "wind_onshore_actual_aggregated",
        ]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        df_aggregated = df.groupby(by=['month', 'day'])[numeric_columns].sum().reset_index()

        df_aggregated["date_label"] = pd.to_datetime(df_aggregated[['month', 'day']].assign(year=2024)).dt.strftime('%b %d')

        fig = go.Figure()

        for energy_type in numeric_columns:
            fig.add_trace(go.Bar(
                x=df_aggregated["date_label"],
                y=df_aggregated[energy_type],
                name=energy_type.replace("_", " ").title()
            ))

        fig.update_layout(
            title="Electricity Generation by Types (Baden Württemberg)",
            xaxis_title="Date",
            yaxis_title="Generated Electricity (MW)",
            barmode="stack",
            legend_title="Energy Type",
            xaxis_tickangle=-45
        )     
        area_fig = fig
        area_fig.update_layout(
            autosize=False,
            width=400,
            height=600,
            margin=dict(
                l=50,
                r=50,
                b=100,
                t=100,
                pad=4
            ),
        )
        st.plotly_chart(area_fig, use_container_width=True, key='area_fig')


csv = filtered_df.to_csv(index=False)
st.download_button(label="Download weather data as CSV", data=csv, file_name='weather_data.csv', mime='text/csv')

csv = load_results.to_csv(index=False)
st.download_button(label="Download load data as CSV", data=csv, file_name='load_data.csv', mime='text/csv')

csv = df.to_csv(index=False)
st.download_button(label="Download electricity generation data as CSV", data=csv, file_name='generation_data.csv', mime='text/csv')
