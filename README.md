Here's an improved version of the README file for your 'RenewableInsight' project to better communicate its purpose, features, and usage:

# RenewableInsight

RenewableInsight is a Streamlit-powered application for analyzing renewable energy trends. The app helps users visualize real-time and historical renewable energy data and provides insights by connecting to Google BigQuery, utilizing current weather and energy pricing information.

## Features

- **Real-time Renewable Energy Analysis**: Integrates weather and energy pricing data for up-to-date insights.
- **Historical Data Visualization**: Interactive charts to track energy trends.
- **BigQuery Integration**: Seamlessly connect and fetch data from Google BigQuery.

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/mohammadp1001/RenewableInsight.git
   ```
2. Install the required dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Setup

1. Create an S3 bucket and a BigQuery dataset to store the required data.
2. Create an account on the ENTSOE website and generate an `ENTSOE_API_KEY`.
3. Create a `.env` file with the following format:
   ```
   # Example .env file
   S3_BUCKET_NAME=<your_s3_bucket_name>
   BIGQUERY_DATASET=<your_bigquery_dataset_name>
   ENTSOE_API_KEY=<your_entsoe_api_key>
   ENTSOE_API_KEY=<your_entsoe_api_key>
   SERVICE_ACCOUNT_FILE=service-account-file.json
   ```
4. Ensure that the `service-account-file.json` for BigQuery exists in the main directory of the project.

5. Build the data pipeline using Docker Compose:
   ```sh
   docker compose --profile kafka up -d
   docker compose --profile producer up -d
   docker compose up server -d
   docker compose up agent -d
   docker compose up orchestrator -d
   ```

6. Once the pipeline is running, confirm it with the Prefect web interface available at `http://localhost:4200`.

## Usage

Run the Streamlit app locally:

```sh
streamlit run app.py
```

Open your web browser at `http://localhost:8501` to access the app.

## Data Sources

- Google BigQuery (renewable energy data)
- Real-time weather API

## Contributing

Feel free to fork the project and submit pull requests.

## License

This project is licensed under the MIT License.

---
Would you like to add any additional sections, such as a detailed example or more information about the intended audience?

