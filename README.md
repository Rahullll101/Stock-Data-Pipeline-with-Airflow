# 📈 Stock Market Data Pipeline with Apache Airflow

[![Docker](https://img.shields.io/badge/Docker-Ready-blue)](#) [![Airflow](https://img.shields.io/badge/Airflow-2.7-brightgreen)](#) [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](#)

A fully automated, Dockerized data pipeline to fetch real-time stock data from Alpha Vantage, process it, and store it in PostgreSQL—scheduled and monitored by Apache Airflow.

This documentation is your comprehensive guide to understanding, deploying, and using **this stock data pipeline** for your analytics, dashboards, or financial automation projects.

---

## 🎯 Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline that:

- **Extracts** daily stock data from the [Alpha Vantage API](https://www.alphavantage.co/documentation/).
- **Transforms** and cleans the data for database storage.
- **Loads** it to a **PostgreSQL** table.
- **Schedules** daily (or custom) updates via **Apache Airflow**.
- **Automates** everything in isolated Docker containers.

**Business Use Cases:**  
Financial dashboards, market analytics, automated trading, portfolio monitoring, and more.

---

## ✨ Features

| Feature                | Description                                |
|------------------------|--------------------------------------------|
| ✅ Fully Dockerized    | One-command deployment                     |
| ✅ Automated Scheduling| Airflow DAG with daily runs                |
| ✅ Error Handling      | Retries, exceptions, logging               |
| ✅ Data Integrity      | UPSERT (no duplicates)                     |
| ✅ Visual Monitoring   | Airflow UI dashboard                       |
| ✅ Secure              | Credentials via environment variables      |
| ✅ Scalable            | Add more stock symbols easily              |
| ✅ Production Ready    | Health checks, logs, modular design        |

---

## 🔧 Technologies Used

| Technology            | Purpose                  |
|-----------------------|--------------------------|
| Docker + Compose      | Containerization         |
| Python 3.11           | ETL logic                |
| Apache Airflow 2.7    | Orchestration            |
| PostgreSQL 15         | Relational database      |
| Alpha Vantage API     | Market data source       |
| psycopg2              | PostgreSQL adapter       |
| requests              | API calls                |

---

## 🏗️ Architecture Overview

**Component Interaction**

```mermaid
flowchart TD
    subgraph docker_compose [Docker Compose]
        subgraph database_layer [Database Layer]
            PG[PostgreSQL]
        end
        subgraph airflow_layer [Airflow Container LocalExecutor]
            AI[Airflow Init one time]
            WS[Airflow Webserver]
            SCH[Airflow Scheduler]
            TASK[Airflow Task Python ETL]
            AI -->|Init metadata db| PG
            WS -.Read metadata.- PG
            SCH --> TASK
        end
    end
    TASK -->|Fetches data| AV[Alpha Vantage API]
    TASK -->|Stores stock data| PG
```

---

## 📦 Prerequisites

1. **Docker Desktop**  
   - [Download for Windows/Mac](https://www.docker.com/products/docker-desktop/)
   - Linux: [Install Docker Engine](https://docs.docker.com/engine/install/)

2. **Alpha Vantage API Key**  
   - Get a free key at: https://www.alphavantage.co/support/#api-key

---

## 🚀 Installation Guide

### 1. Clone or Create Project Structure

```bash
mkdir stock-pipeline
cd stock-pipeline
mkdir dags scripts init-db
```

### 2. Prepare the `.env` File

Create `.env` in the root:

```env
# PostgreSQL Configuration
POSTGRES_USER=YOUR_POSTGRES_USER
POSTGRES_PASSWORD=YOUR_POSTGRES_PASSWORD
POSTGRES_DB=YOUR_DATABASE_NAME

# Airflow Admin
AIRFLOW_USER=YOUR_AIRFLOW_USERNAME
AIRFLOW_PASSWORD=YOUR_AIRFLOW_PASSWORD

# Alpha Vantage API Key
ALPHA_VANTAGE_API_KEY=YOUR_API_KEY_HERE

# Airflow Webserver Secret Key (generate one!)
AIRFLOW__WEBSERVER__SECRET_KEY=YOUR_GENERATED_SECRET_KEY
```

**Important:**  
- Replace placeholders with your real values.
- Do **not** use quotes or extra spaces around `=`.
- **Never commit your `.env` file to public repositories.**

---

## 🗃️ Project Structure

```bash
stock-pipeline/
├── docker-compose.yml
├── .env
├── .gitignore
├── README.md
├── requirements.txt
├── dags/
│   └── stock_data_pipeline.py
├── scripts/
│   └── fetch_stock_data.py
└── init-db/
    └── init.sql
```

---

## 🏁 Quick Start

1. **Start all services:**

   ```bash
   docker compose up -d
   ```

2. **Check containers:**

   ```bash
   docker compose ps
   ```

3. **Access Airflow UI:**  
   Open [http://localhost:8080](http://localhost:8080)  
   Login:  
   - Username: from `.env` (`AIRFLOW_USER`)
   - Password: from `.env` (`AIRFLOW_PASSWORD`)

4. **Trigger the DAG**  
   - Find `stock_data_pipeline`.
   - Toggle it ON.
   - Click ▶ to run.

---

## 📊 Validating the Pipeline

### Method 1: Airflow UI

- Check that the DAG run status is **green** (success).
- Inspect task logs if failures occur.

### Method 2: PostgreSQL Data

- Connect:

  ```bash
  docker compose exec postgres psql -U YOUR_POSTGRES_USER -d YOUR_DATABASE_NAME
  ```

- Query:

  ```sql
  SELECT COUNT(*) FROM stock_data;
  SELECT * FROM stock_data ORDER BY date DESC LIMIT 10;
  ```

- Exit:

  ```bash
  \q
  ```

---

## 🗄️ Database Schema

```sql
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    volume BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);
CREATE INDEX idx_symbol_date ON stock_data(symbol, date);
```

**Features:**

- **Unique Constraint:** Prevents duplicate stock entries per symbol and date.
- **Index:** Speeds up queries filtering by symbol and date.

```mermaid
erDiagram
    stock_data {
        SERIAL id PK
        VARCHAR symbol
        DATE date
        NUMERIC open
        NUMERIC high
        NUMERIC low
        NUMERIC close
        BIGINT volume
        TIMESTAMP updated_at
    }
```

---

## ⚙️ Configuration

### Change Schedule

Edit `dags/stock_data_pipeline.py`:

```python
schedule_interval='@daily'  # Options: '@hourly', '0 9 * * *', etc.
```

Restart Airflow to apply the new schedule:

```bash
docker compose restart airflow-scheduler airflow-webserver
```

### Add More Stocks

Edit `scripts/fetch_stock_data.py`:

```python
symbols = ['IBM', 'AAPL', 'GOOGL', 'MSFT']
```

- ⚠️ **Alpha Vantage free tier:** Max 25 API calls per day and 5 per minute.

---

## 📝 File-by-File Documentation

---

### 1. `.env` – Environment Variables

This file defines all sensitive credentials and runtime configuration for the stack.

**Key Variables:**

| Variable                      | Purpose                                 |
|-------------------------------|-----------------------------------------|
| POSTGRES_USER                 | PostgreSQL username                     |
| POSTGRES_PASSWORD             | PostgreSQL password                     |
| POSTGRES_DB                   | Database name                           |
| AIRFLOW_USER                  | Airflow admin username                  |
| AIRFLOW_PASSWORD              | Airflow admin password                  |
| ALPHA_VANTAGE_API_KEY         | API key for Alpha Vantage               |
| AIRFLOW__WEBSERVER__SECRET_KEY| Secret for Airflow webserver security   |

**Best Practices:**

- Never publish this file.
- No quotes or whitespace with `=`.
- Regenerate secret keys with:

  ```bash
  python -c "import secrets; print(secrets.token_hex(32))"
  ```

---

### 2. `docker-compose.yml` – Container Orchestration

This file launches all project services in coordinated Docker containers.

**Main Services:**

| Service            | Role                   | Key Configs               |
|--------------------|------------------------|---------------------------|
| postgres           | Database               | Ports, volumes, health    |
| airflow-init       | Initialize Airflow DB  | Waits for DB, migrations  |
| airflow-webserver  | Airflow UI/API         | Exposes port 8080         |
| airflow-scheduler  | Executes DAGs          | Runs scheduled tasks      |

**Key Features:**

- Volume mounts for data persistence and code hot-reload.
- Environment variables loaded from `.env`.
- Health checks for reliable startup and dependencies.

**Partial Example:**

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-db:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  airflow-init:
    image: apache/airflow:2.7.3-python3.11
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/${POSTGRES_DB}
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${AIRFLOW_USER}
      _AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_PASSWORD}
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
      - ./requirements.txt:/requirements.txt
    command: >
      bash -c "pip install -r /requirements.txt &&
      airflow db migrate &&
      airflow users create
      --username ${AIRFLOW_USER}
      --password ${AIRFLOW_PASSWORD}
      --firstname Admin
      --lastname User
      --role Admin
      --email admin@example.com || true"
```

---

### 3. `requirements.txt` – Python Dependencies

This file lists the Python libraries required for the ETL logic.

```txt
requests==2.31.0
psycopg2-binary==2.9.9
```

- `requests`: For HTTP requests to the Alpha Vantage API.
- `psycopg2-binary`: For connecting to PostgreSQL from Python.

---

### 4. `init.sql` – Database Schema Initialization

This script initializes the PostgreSQL database when the container first starts.

**Creates:**

- `stock_data` table with correct data types for each column.
- Unique constraint to prevent duplicate symbol/date rows.
- Index on `(symbol, date)` for fast lookups.

---

### 5. `dags/stock_data_pipeline.py` – Airflow DAG Definition

This file defines the Airflow DAG that orchestrates the ETL pipeline.

**Main Elements:**

- **DAG Metadata:** Includes `start_date`, `owner`, `retries`, and `retry_delay`.
- **Scheduling:** Uses `schedule_interval` for daily or custom schedules.
- **Task:** A single `PythonOperator` calling `fetch_and_store_stock_data`.

**Key Snippet:**

```python
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['stock', 'market-data'],
)

fetch_task = PythonOperator(
    task_id='fetch_and_store_stock_data',
    python_callable=fetch_and_store_stock_data,
    dag=dag,
)
```

---

### 6. `scripts/fetch_stock_data.py` – Stock Data ETL Logic

This script contains the core ETL logic used by the Airflow DAG.

#### Key Functions

- **get_db_connection()**: Creates and returns a connection to PostgreSQL.
- **fetch_stock_data(symbol)**: Calls Alpha Vantage API and validates the response.
- **parse_and_store_data(data, symbol)**: Parses JSON data, builds records, and performs UPSERT operations.
- **fetch_and_store_stock_data()**: Orchestrates fetching and storing for use in Airflow.

#### Main Flow

```mermaid
flowchart TD
  A[Start Pipeline] --> B[Fetch Data from Alpha Vantage]
  B --> C[Parse and Prepare Data]
  C --> D[Insert or Upsert into PostgreSQL]
  D --> E[Log Success or Errors]
```

#### Error Handling

- Retries are managed by Airflow based on DAG settings.
- Logs are written for network issues, API errors, and database failures.
- Invalid records are skipped with warnings rather than failing everything.

---

## ⚠️ Troubleshooting

- **Ports in use:** Change `8080` to another free port in `docker-compose.yml` if the UI fails to start.
- **API Key errors:** Confirm `.env` values, ensure no quotes or extra spaces, then restart containers.
- **Rate limit issues:** Avoid triggering the DAG too often on the free Alpha Vantage tier.
- **Data not updating:** Check Airflow and PostgreSQL logs:

  ```bash
  docker compose logs airflow-scheduler
  docker compose logs postgres
  ```

- **Database connection problems:** Ensure `POSTGRES_HOST` is `postgres` in the container environment.

---

## 📋 Assignment Checklist

| Requirement            | Status |
|------------------------|--------|
| Docker Compose         | ✅     |
| Airflow Orchestration  | ✅     |
| API Fetching           | ✅     |
| JSON Parsing           | ✅     |
| PostgreSQL Integration | ✅     |
| Error Handling         | ✅     |
| Env Vars               | ✅     |
| Scheduling             | ✅     |
| Logging                | ✅     |
| README/Docs            | ✅     |

---

## 🔗 Additional Resources

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Alpha Vantage Docs](https://www.alphavantage.co/documentation/)
- [Docker Docs](https://docs.docker.com/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)

---

## 📄 License

MIT License — Free to use, modify, and learn.

---

# API Documentation

The only data-exchange API is with **Alpha Vantage** (external).  
The Airflow webserver exposes a UI at `/`, not a public REST API for this project.

## Example: Fetch Daily Stock Data (Alpha Vantage)

### Alpha Vantage - Time Series Daily

#### Endpoint: Fetch Daily Stock Data

```api
{
    "title": "Fetch Daily Stock Data",
    "description": "Retrieve daily time series of a given stock symbol from Alpha Vantage.",
    "method": "GET",
    "baseUrl": "https://www.alphavantage.co",
    "endpoint": "/query",
    "headers": [],
    "queryParams": [
        {
            "key": "function",
            "value": "TIME_SERIES_DAILY",
            "required": true
        },
        {
            "key": "symbol",
            "value": "IBM",
            "required": true
        },
        {
            "key": "apikey",
            "value": "Your Alpha Vantage API key",
            "required": true
        },
        {
            "key": "outputsize",
            "value": "compact",
            "required": false
        }
    ],
    "pathParams": [],
    "bodyType": "none",
    "requestBody": "",
    "formData": [],
    "responses": {
        "200": {
            "description": "Stock data returned as JSON.",
            "body": "{\n  \"Meta Data\": { ... },\n  \"Time Series (Daily)\": {\n    \"2024-01-01\": {\n      \"1. open\": \"140.00\",\n      \"2. high\": \"142.00\",\n      \"3. low\": \"139.00\",\n      \"4. close\": \"141.50\",\n      \"5. volume\": \"1234567\"\n    },\n    \"2023-12-29\": { ... }\n  }\n}"
        },
        "400": {
            "description": "Invalid parameters or missing API key.",
            "body": "{\n  \"Error Message\": \"Invalid API call. Please retry or visit the documentation for TIME_SERIES_DAILY.\"\n}"
        },
        "429": {
            "description": "API Rate limit exceeded.",
            "body": "{\n  \"Note\": \"Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 calls per day.\"\n}"
        }
    }
}
```

---

```card
{
  "title": "Production Caution",
  "content": "Alpha Vantage free tier is limited, so avoid frequent schedules or too many symbols in one pipeline."
}
```

---

# 🎉 You're Ready!

You now have a robust, production-ready data pipeline template for stock market analytics.  
You can modify stock symbols, adjust the schedule, and extend this foundation for your own use cases.