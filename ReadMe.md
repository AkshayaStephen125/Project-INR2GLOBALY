




# 🌍 INR2GLOBALY

A complete data engineering and analytics pipeline built using **Apache Airflow**, **PostgreSQL**, **pgAdmin**, **Docker**, and **Power BI**.

This project automates the extraction, loading, and transformation of INR-related currency data using scheduled Airflow DAGs, stores it in PostgreSQL, and visualizes insights through an interactive Power BI report.

---

## 🚀 Features

* **Automated ETL Pipeline** using Apache Airflow
* **Containerized Workflow** with Docker & Docker Compose
* **PostgreSQL Database** for structured storage
* **pgAdmin UI** connected via localhost
* **Airflow DAGs** that fetch, clean, and load currency data
* **Power BI Dashboard** for analysis & trends visualization
* End-to-end reproducible & scalable data pipeline

---

## 🛠️ Technology Stack

| Component              | Technology             |
| ---------------------- | ---------------------- |
| Workflow Orchestration | Apache Airflow         |
| Data Storage           | PostgreSQL             |
| DB Admin               | pgAdmin                |
| Containerization       | Docker, Docker Compose |
| Analytics              | Power BI               |
| Programming Language   | Python                 |

---


## 🧱 Architecture Overview

1. **Docker Compose** spins up:

   * Airflow Scheduler
   * Airflow Webserver
   * PostgreSQL
   * Airflow DAG Processor

2. **Airflow DAG** runs on schedule:

   * Creates a master table and loads master data if not exists
   * Fetches currency data (API)
   * Transforms and validates data
   * Loads it into PostgreSQL tables

3. **pgAdmin** is utilized as the local PostgreSQL administration client instead of the Docker container because Power BI cannot connect to the pgAdmin container. All tables are managed through the local setup.

4. **Power BI** connects to PostgreSQL and visualizes:

   * Daily currency variations
   * INR vs global currencies
   * Latest exchange rates

---

## ▶️ How to Run the Project

### **1. Clone the repository**

```bash
git clone https://github.com/AkshayaStephen125/Project-INR2GLOBALY.git
cd INR2-Globally
```

### **2. Start Docker services**

```bash
docker-compose up -d
```

### **3. Setup local pgadmin**

- Set environment variables for Airflow to connect to local Postgres
```bash
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://postgres:1269@host.docker.internal:5432/inr_db**
AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://postgres:1269@host.docker.internal:5432/inr_db
```

- Initialize Airflow Database
```bash
docker-compose exec airflow-apiserver airflow db migrate
```

- Create Admin User
```bash
docker-compose exec airflow-apiserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

### **4. Access Services**

* **Airflow UI** → [http://localhost:8080](http://localhost:8080)
* **Airflow Credentials**

  * Username: `admin`
  * Password: `admin`

### **5. Add PostgreSQL Connection in Airflow**

To configure the database connection used by the DAG, add a new Airflow connection named **`currency_connection`**:

- **Conn Id:** `currency_connection`
- **Conn Type:** `Postgres`
- **Host:** `host.docker.internal`
- **Port:** `5432`
- **User:** `postgres`
- **Password:** `1269`
- **Database (Schema):** `inr_db`
- **Extra:** *(leave empty)*

This enables Airflow to load currency data into the PostgreSQL database running on the host machine.

### **6. Trigger the DAG**

* Open Dags Airflow UI
* Enable and trigger `extract_and_load_currency_data`

### **7. View Loaded Data**

* Open **pgAdmin**.
* Connect to **PostgreSQL**.
* Open the database **`inr_db`**.
* Expand the **`public`** schema.
* Check the table **`inr_currency`** to view the loaded currency data.

You can also run a quick query in pgAdmin SQL editor:

```sql
SELECT * FROM inr_currency LIMIT 10;
```

This will display the first 10 rows of the table for verification.


### **8. Analyze in Power BI**

* Use PostgreSQL connector
* Load tables:

  * `master_currency`
  * `inr_currency`

Refresh to get updated DAG-output data.

---

## 📊 Power BI Dashboard

The dashboard includes:

* INR vs major world currencies trend lines
* Day-by-day variation analysis
* Latest rate snapshot
* Interactive filters (date, country, range)

---


## 📝 Airflow DAG Overview

* **Task 1:** Check master table & create if not exists
* **Task 2:** Insert master data
* **Task 3:** Fetch currency data
* **Task 4:** Create currency table if not exists
* **Task 5:** Insert currency data into table

Retry logic and task dependencies ensure pipeline stability.

---

## 📦 Requirements

* Docker
* Docker Compose
* Power BI Desktop
* PostgreSQL ODBC driver 
---

## 🤝 Contributing

Pull requests are welcome!
For major changes, open an issue first to discuss what you want to improve.

---

## 📄 License

This project is for personal use only. All rights reserved.

---

## 👤 Author

**Akshaya Stephen**
Built with ❤️ using Data Engineering + Power BI
