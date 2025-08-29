# 🌧️ OpenWeather Pipeline

An **end-to-end data engineering pipeline** that ingests live weather data from the OpenWeather API, processes it into **Bronze → Silver → Gold layers** using Apache Spark, and orchestrates everything with Apache Airflow.  

The project is currently configured for **local execution** with Astronomer (Astro CLI) but is designed to be production-ready for deployment on **Azure Databricks + Azure Storage** in the future.

---

# 📖 Overview

This project demonstrates:
- 🌍 **Ingestion** of hourly weather data from the OpenWeather API.
- 🪙 **Bronze layer**: Raw JSON data is ingested and stored.
- ⚙️ **Silver layer**: Spark transformations to clean & normalize the data.
- 🏅 **Gold layer**: Feature engineering for ML models (e.g., rainfall prediction).
- 🎯 **Airflow DAGs** to orchestrate the full pipeline.

Currently, the pipeline is **manually triggered** in Airflow. Future work will enable **hourly scheduling** in the cloud.

---

# 🚀 Features

- 🔑 **Secure API Access** using environment variables / Airflow connections.
- 🔄 **Retries with Backoff** for resilient API calls.
- ⚡ **Spark-based transformations** for scalable ETL.
- 🗂️ **Medallion architecture** (Bronze → Silver → Gold).
- 🐳 **Dockerized Airflow** environment with Astronomer (Astro CLI).
- 📊 **ML-ready gold dataset** (binary rain/no-rain classification).

---

# 📂 Project Structure

```bash
openweather-pipeline/
├── airflow/                      # Airflow project (Astro CLI managed)
│   ├── .dockerignore
│   ├── .env
│   ├── .gitignore
│   ├── airflow_settings.yaml      # Airflow connections & variables (local dev)
│   ├── docker-compose.override.yaml
│   ├── Dockerfile
│   ├── packages.txt
│   ├── README.md
│   ├── requirements.txt
│   ├── .astro/                   # Astronomer configs
│   ├── dags/
│   │   ├── .airflowignore
│   │   └── rain_pipeline.py       # Main DAG: Ingest → Bronze → Silver → Gold
│   ├── rain_pipeline/
│   │   ├── __init__.py
│   │   ├── config/
│   │   │   └── populated_places.json  # Geo data for API calls
│   │   ├── ingestion/
│   │   │   ├── __init__.py
│   │   │   └── fetch_current.py   # Ingestion script (OpenWeather → Bronze)
│   │   ├── scripts/
│   │   │   └── utils.py           # Logging, HTTP client, API key utils
│   │   └── spark/
│   │       ├── __init__.py
│   │       ├── schemas/
│   │       │   ├── __init__.py
│   │       │   └── schema_silver_current.py
│   │       └── transforms/
│   │           ├── __init__.py
│   │           ├── bronze_to_silver_transform.py
│   │           └── silver_to_gold_transform.py
│   └── tests/
├── .gitignore
├── LICENSE
├── poetry.lock
├── pyproject.toml
├── README.md                     # Project documentation
```

---

# 🧪 Usage

### 1️⃣ Start Airflow locally with Astro CLI
```bash
cd airflow
astro dev start
```

### 2️⃣ Access the Airflow UI
- Open: [http://localhost:8080](http://localhost:8080)
- Login: `airflow / airflow` (default)

### 3️⃣ Trigger the DAG
- In the Airflow UI, trigger **`rain_pipeline`**
- This runs:  
  - `fetch_current.py` → writes **Bronze JSON files**  
  - `bronze_to_silver_transform.py` → cleans data into **Silver parquet**  
  - `silver_to_gold_transform.py` → creates ML-ready **Gold parquet**

---

# 🔭 Future Work

- ⏰ **Hourly scheduling** for ingestion.  
- ☁️ **Deploy to Azure Databricks + ADLS** for production-scale ETL.  
- 🔐 Use **Azure Key Vault** for secret management.  
- 📈 Build ML models (binary rain/no-rain classification).  

---

# 🧩 Contributing
Contributions, issues, and PRs are welcome! 🚀  

---

# 📫 Contact

- **Author**: Arif A. Othman  
- **Email**: [arithman34@hotmail.com](mailto:arithman34@hotmail.com)  
- **GitHub**: [arithman34](https://github.com/arithman34)  
- **LinkedIn**: [arithman34](https://www.linkedin.com/in/arithman34/)

---

# 📜 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
