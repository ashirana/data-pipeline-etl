# 📊 Data Pipeline: API → PostgreSQL (ETL)

## 📌 Overview

This project demonstrates a **batch ETL (Extract, Transform, Load) pipeline** built using Python.

The pipeline fetches data from a public API, processes it using Pandas, and loads it into a PostgreSQL database for storage and analysis.

---

## 🏗️ Architecture

```
API (JSONPlaceholder)
        ↓
Python Ingestion Script
        ↓
Pandas Transformation
        ↓
PostgreSQL Database
```

---

## ⚙️ Tech Stack

* **Python**
* **Pandas**
* **PostgreSQL**
* **SQLAlchemy**
* **Requests**

---

## 🔄 Pipeline Workflow

### 1. Extract

* Fetch data from API:

  ```
  https://jsonplaceholder.typicode.com/comments
  ```

### 2. Transform

* Convert JSON → Pandas DataFrame
* Add metadata column:

  * `ingestion_time`

### 3. Load

* Insert data into PostgreSQL table:

  ```
  comment_table
  ```
* Uses **append mode** to preserve historical data

---

## 📂 Project Structure

```
data_pipeline_project/
│── ingest.py
│── README.md
│── requirements.txt
│── .gitignore
```

---

## 🚀 How to Run

### 1. Clone Repository

```
git clone https://github.com/<your-username>/data-pipeline-etl.git
cd data-pipeline-etl
```

---

### 2. Create Virtual Environment

```
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Install Dependencies

```
pip install -r requirements.txt
```

---

### 4. Setup PostgreSQL

Ensure PostgreSQL is running locally:

```
username: postgres
password: postgres
host: localhost
port: 5432
database: postgres
```

---

### 5. Run Pipeline

```
python ingest.py
```

---

## 🧪 Output

* Table created: `comment_table`
* Data successfully loaded from API
* Includes ingestion timestamp

---

## ⚠️ Known Issues

* Duplicate data on multiple runs (append mode)
* No primary key constraint
* No upsert logic

---

## 📈 Future Improvements

* Implement **upsert (ON CONFLICT)**
* Add **Kafka for streaming ingestion**
* Add **Airflow for scheduling**
* Containerize using **Docker**
* Add logging & monitoring

---

## 🧠 Key Learnings

* Built an end-to-end ETL pipeline
* Understood data ingestion concepts
* Worked with PostgreSQL using SQLAlchemy
* Managed Python environments using venv
* Version control using Git & GitHub

---

## 👨‍💻 Author

**Ashish Ashish**
