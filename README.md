# Dual-Pipeline Data Platform

A production-grade data pipeline project built with Apache Airflow, 
dbt, and GCP : demonstrating both ETL and ELT patterns side by side.

![CI Pipeline](https://github.com/priyansh16/Airflow-pipelines/actions/workflows/ci.yml/badge.svg)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Apache Airflow                       │
│              (Orchestration & Scheduling)               │
└──────────────────┬──────────────────┬───────────────────┘
                   │                  │
      ┌────────────▼──────┐  ┌────────▼────────────┐
      │   ETL Pipeline    │  │    ELT Pipeline     │
      │  Financial News   │  │  E-commerce Events  │
      │  Sentiment        │  │  Analytics          │
      └────────────┬──────┘  └────────┬────────────┘
                   │                  │
      ┌────────────▼──────────────────▼────────────┐
      │           Google Cloud Storage             │
      │              (Raw landing zone)            │
      └──────────────────────────────┬─────────────┘
                                     │
      ┌──────────────────────────────▼─────────────┐
      │                 BigQuery                   │
      │   raw → (dbt) → staging → (dbt) → marts    │
      └──────────────────────────────┬─────────────┘
                                     │
      ┌──────────────────────────────▼─────────────┐
      │            Looker Studio Dashboard         │
      └────────────────────────────────────────────┘
```

## Pipelines

| Pipeline | Pattern | Source | Schedule | Rows/run |
|---|---|---|---|---|
| Financial news sentiment | ETL | Alpha Vantage API | Every 6 hours | ~50 articles |
| E-commerce analytics | ELT + dbt | Synthetic (Faker) | Daily midnight | 500 events |

## Why ETL for news and ELT for e-commerce?

**ETL** made sense for the news pipeline because the core transformation 
is Python-heavy sentiment scoring (VADER). This logic does not belong in SQL 
and should happen before the data enters the warehouse.

**ELT** made sense for e-commerce because the transformations are pure SQL 
aggregations that BigQuery executes far more efficiently than Python. dbt gives 
us version-controlled, tested business logic that lives alongside the warehouse.

## Tech stack


| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 (TaskFlow API) |
| Warehouse | Google BigQuery |
| Storage | Google Cloud Storage |
| Transformation | dbt (staging + mart layers) |
| Data quality | dbt schema tests (15 tests) |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
| Alerting | Slack webhooks |
| Dashboard | Looker Studio |
| Local dev | Docker Compose |

## Live dashboard

[View the Looker Studio dashboard](https://datastudio.google.com/reporting/36597182-cc02-433e-a3a9-8c9044b1936e)

## Local setup

**Prerequisites:** Docker Desktop, Python 3.11+, gcloud CLI

```bash
# 1. Clone the repo
git clone https://github.com/priyansh16/Airflow-pipelines.git
cd Airflow-pipelines

# 2. Add your GCP service account key
cp /path/to/your/keyfile.json include/gcp_keyfile.json

# 3. Start Airflow
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler

# 4. Open http://localhost:8080 (admin/admin)
```

## Project structure

```
├── dags/
│   ├── etl/
│   │   └── financial_news_etl.py   # ETL pipeline
│   └── elt/
│       └── ecommerce_elt.py        # ELT pipeline
├── dbt/
│   ├── ecommerce_dbt/
│   │    └── models/
│   │       ├── staging/            # Cleaning layer
│   │       └── marts/              # Business-ready tables
    └── profiles.yml
├── include/
│   ├── generate_events.py          # Synthetic data generator
│   └── callbacks.py                # Slack alerting
├── terraform/
│   └── main.tf                     # GCP infrastructure as code
├── tests/
│   └── test_dag_integrity.py       # CI tests
├── docker-compose.yml
├── requirements.txt                # env setup file
└── run_tests.sh                    # runs dag intigrity pytest
```

## Design decisions

**Why GCP over AWS?**

My other project [Swedish Property Price Predictor](https://github.com/priyansh16/PricePrediction) uses AWS. Demonstrating 
multi-cloud fluency reflects real enterprise environments where teams 
routinely work across providers.

**Why both sentiment scores?**

The Alpha Vantage API provides its own sentiment score, but I also run 
VADER locally. The API score is a black box — if they update their model 
silently, historical comparisons break. Owning our scoring logic gives 
reproducibility and vendor independence. Storing both columns in BigQuery 
lets us measure model agreement, which surfaced an interesting finding: 
VADER systematically over-scores financial headlines because it was trained 
on social media text, not financial journalism.

**Why Terraform?**

Infrastructure as code means the entire GCP environment is reproducible 
with one command. A new engineer can spin up an identical environment 
without clicking through the GCP console.

## Running tests

```bash
./run_tests.sh
```