# RenewableInsight

RenewableInsight is a Streamlit-powered application for analyzing renewable energy trends. This is a demo app to visualize real-time and historical renewable energy data, utilizing current weather and energy pricing information.


## Technologies Used

- **Programming Languages**: Python
- **Frontend**: Streamlit
- **Cloud Services**: AWS S3, Google BigQuery
- **Data Pipeline**: Apache Kafka, Docker, Prefect
- **Infrastructure as Code**: Terraform



## How to Setup

1. Create an S3 bucket and a BigQuery dataset to store the required data. You can easily set up these cloud resources using the provided Terraform file:

   Navigate to the directory containing the Terraform file and initialize the environment:

   ```sh
   cd terraform
   terraform init
   ```

   Apply the Terraform configuration to create the resources:

   ```sh
   terraform apply
   ```

   This will create:
   - An AWS S3 bucket to store renewable energy data.
   - A Google BigQuery dataset to store energy analysis data.

2. Create an account on the ENTSOE website and generate an `ENTSOE_API_KEY`.

3. Ensure that the `service-account-file.json` for BigQuery exists in the main directory of the project.

4. Edit the `env.env` file with the information you have from previous steps.

5. Build the data pipeline using Docker Compose:

   ```sh
   docker compose --profile kafka up -d
   docker compose --profile producer up -d
   docker compose up server -d
   docker compose up agent -d
   docker compose up orchestrator -d
   ```

   Logs for the running services can be checked with:

   ```sh
   docker compose logs -f
   ```

6. Once the pipeline is running, confirm it with the Prefect web interface available at `http://localhost:4200`.

7. You can also check the Kafka broker with the web interface available at `http://localhost:9021`.


8. Run the Streamlit app locally:

    ```sh
   streamlit run ui/ui_generator.py
    ```

9. Open your web browser at `http://localhost:8501` to access the app.

10. You can also access the already deployed version of ui: https://renewableinsight-bw.streamlit.app/

## Data Sources

- [ENTSOE](https://www.entsoe.eu) (electricity data)
- [Yahoo Finance](https://finance.yahoo.com) (energy pricing data)
- [Deutscher Wetterdienst (DWD)](https://www.dwd.de) (weather data)


---

