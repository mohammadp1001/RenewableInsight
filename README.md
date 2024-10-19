# RenewableInsight

RenewableInsight is a Streamlit-based web application that provides valuable insights into renewable energy trends, real-time weather conditions, and energy pricing information. The application leverages data from Google BigQuery to deliver accurate, up-to-date information and analytics.

## Features

- **Real-Time Data Analysis**: The app fetches real-time weather and energy pricing information to provide an accurate overview of current renewable energy trends.
- **Historical Data Visualization**: Users can explore historical data for renewable energy production and consumption, visualized through easy-to-understand charts and graphs.
- **BigQuery Integration**: The app pulls data from Google BigQuery, ensuring reliable and scalable data management.
- **User-Friendly Interface**: Built with Streamlit, the app provides an intuitive interface that makes data visualization simple and accessible.

## Getting Started

### Prerequisites

- Python 3.7 or higher
- Google Cloud SDK (for BigQuery integration)
- Streamlit

To install the necessary Python packages, use the following command:

```sh
pip install -r requirements.txt
```

### Setting Up BigQuery

To connect the application to Google BigQuery, follow these steps:

1. Create a Google Cloud project and enable the BigQuery API.
2. Set up authentication by creating a service account key. Download the JSON key file.
3. Set the environment variable to point to your BigQuery credentials:

   ```sh
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-file.json"
   ```

### Running the Application

To run the Streamlit app locally, use the following command:

```sh
streamlit run app.py
```

### Directory Structure

- `app.py` - The main application file containing the Streamlit code.
- `bigquery_helper.py` - Contains helper functions to interact with Google BigQuery.
- `requirements.txt` - Lists all the dependencies required for the project.

## Usage

Once the application is running, you can access it via your web browser at `http://localhost:8501`. The interface allows you to:

- View current renewable energy production statistics.
- Analyze trends based on historical data.
- Compare different regions or types of renewable energy.

## Configuration

You can configure different settings, such as API keys or visualization preferences, by modifying the configuration file (`config.json`). This file includes parameters for accessing BigQuery tables and setting visualization options.

## Deployment

To deploy the app, you can use services like [Streamlit Cloud](https://streamlit.io/cloud) or [Heroku](https://www.heroku.com/). Make sure to include your BigQuery credentials and adjust the configuration as needed.

### Deploying on Streamlit Cloud

1. Push your code to a GitHub repository.
2. Link the repository to your Streamlit Cloud account.
3. Set the environment variables for Google BigQuery credentials.

### Deploying on Heroku

1. Create a Heroku app.
2. Set up environment variables via the Heroku dashboard or CLI.
3. Deploy the code via Git or Heroku CLI.

## Contributing

Contributions are welcome! Feel free to submit issues or pull requests to improve the project. Before contributing, please read the [contribution guidelines](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For any inquiries or questions, please reach out to Mohammad Pakdaman at [mohammad@example.com](mailto:mohammad@example.com).

## Acknowledgements

- Google BigQuery for providing robust data services.
- Streamlit for the web application framework.
- OpenWeatherMap for weather data (if applicable).

---

Feel free to customize this README further to suit the specific aspects of your project!
