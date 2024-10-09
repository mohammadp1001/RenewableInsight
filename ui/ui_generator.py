import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery

path_to_append = os.getenv('PYTHON_APP_PATH')
if path_to_append:
    sys.path.append(path_to_append)

from src.config import Config

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)


credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)

st.set_page_config(
    page_title="Renewable Energy Insight",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")


col1, col2, col3 = st.columns(3)

# Left Column: Electricity Load Information
with col1:
    st.subheader('Total Electricity Load (Last 5 Days)')
    st.bar_chart(load_data_last_5_days.set_index('Day'))

    st.subheader('Load by Hour (Last 24 Hours)')
    st.bar_chart(hourly_load_data.set_index('Hour'))

# Middle Column: Generation Information
with col2:
    st.subheader('Electricity Generation Types')
    
    # Pie chart for generation types
    fig, ax = plt.subplots()
    ax.pie(generation_data.values(), labels=generation_data.keys(), autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    st.pyplot(fig)

    st.subheader('Average of Last 3 Days Generation Types')
    st.bar_chart(pd.DataFrame(generation_data.items(), columns=['Type', 'Percentage']).set_index('Type'))

# Right Column: Prices and Weather Information
with col3:
    st.subheader('Weather Forecast for Next 3 Days')
    st.write('Solar:', weather_data['Solar'])
    st.write('Wind:', weather_data['Wind'])

    st.subheader('Prices (Gas, Coal, Oil)')
    st.write('Gas:', prices_data['Gas'])
    st.write('Coal:', prices_data['Coal'])
    st.write('Oil:', prices_data['Oil'])
