<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:1a73e8,100:34a853&height=200&section=header&text=Data%20Engineering%20Zoomcamp%202026&fontSize=40&fontColor=ffffff&fontAlignY=38&desc=Homework%20solutions%20%26%20end-to-end%20projects&descAlignY=58&descSize=18" alt="banner" width="100%" />

# 🛠️ Data Engineering Zoomcamp 2026

My homework solutions and projects for the [**Data Engineering Zoomcamp 2026**](https://github.com/DataTalksClub/data-engineering-zoomcamp) — a free course by [DataTalks.Club](https://datatalks.club/) covering the modern data engineering stack.

[![Course](https://img.shields.io/badge/DataTalks.Club-Zoomcamp_2026-1a73e8?style=flat-square)](https://datatalks.club/)
![Modules](https://img.shields.io/badge/Modules-7%2F7_complete-34a853?style=flat-square)
![Workshop](https://img.shields.io/badge/Workshop-dlt-success?style=flat-square)
![Project](https://img.shields.io/badge/Capstone-SoCal_NOD_Tracker-blueviolet?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-lightgrey?style=flat-square)

<br/>

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=for-the-badge&logo=terraform&logoColor=white)

</div>

---

## 📚 Homework Solutions

| # | Module | Topic | Tech Stack |
|:-:|--------|-------|------------|
| 1 | [Module1](Module1/) | **Docker & SQL** | Docker, PostgreSQL, Terraform, GCP |
| 2 | [Module2](Module2/) | **Workflow Orchestration** | Kestra |
| 3 | [Module3](Module3/) | **Data Warehouse** | BigQuery, dlt |
| 4 | [Module4](Module4/) | **Analytics Engineering** | dbt, dimensional modeling |
| 5 | [Module5](Module5/) | **Data Platforms** | Bruin |
| 6 | [Module6](Module6/) | **Batch Processing** | Apache Spark (PySpark) |
| 7 | [Module7](Module7/) | **Streaming** | PyFlink, Redpanda |
| 🧪 | [Workshop](Workshop/) | **dlt Workshop** | dlt, DuckDB, REST API |

> Each module folder contains a `homework.md` with the questions, my answers, and the code/queries used to derive them.

## 🚀 Capstone Project

### [SoCal NOD Tracker](project1/) — Foreclosure Early-Warning Pipeline

An end-to-end pipeline tracking **Notice of Default (NOD)** filings across 6 Southern California counties — a leading indicator of foreclosure activity.

```
Daily CSVs → Kestra DAG → GCS (data lake) → BigQuery (raw) → dbt (marts) → Looker Studio
```

**Stack:** Terraform · Kestra · Google Cloud Storage · BigQuery · dbt · Looker Studio

See the [full project README](project1/README.md) for architecture, reproduction steps, and the dashboard.

## 🧰 Tools & Technologies

| Layer | Tools |
|-------|-------|
| **Containerization** | Docker, Docker Compose |
| **Orchestration** | Kestra |
| **Data Lake** | Google Cloud Storage |
| **Data Warehouse** | BigQuery, PostgreSQL, DuckDB |
| **Ingestion** | dlt |
| **Transformation** | dbt, Bruin |
| **Batch Processing** | Apache Spark / PySpark |
| **Streaming** | Apache Flink (PyFlink), Redpanda |
| **Infrastructure as Code** | Terraform |
| **Visualization** | Looker Studio |

## 📂 Repository Structure

```
.
├── Module1/        # Docker & SQL
├── Module2/        # Workflow Orchestration (Kestra)
├── Module3/        # Data Warehouse (BigQuery, dlt)
├── Module4/        # Analytics Engineering (dbt)
├── Module5/        # Data Platforms (Bruin)
├── Module6/        # Batch Processing (Spark)
├── Module7/        # Streaming (PyFlink, Redpanda)
├── Workshop/       # dlt Workshop
└── project1/       # Capstone: SoCal NOD Tracker
```

## 🎓 About the Course

The [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) is a free, hands-on course covering data engineering fundamentals: containerization, workflow orchestration, data warehousing, analytics engineering, batch processing, and streaming.

## 👤 Author

**Michael** — [@HighviewOne](https://github.com/HighviewOne)

## 📄 License

Released under the [MIT License](LICENSE).

<div align="center">
<sub>⭐ If you find this useful, consider giving it a star!</sub>
</div>
