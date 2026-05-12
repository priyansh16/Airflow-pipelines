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

<style>
.stack-wrap{display:flex;flex-wrap:wrap;gap:10px;padding:8px 0}
.badge{display:flex;align-items:center;gap:8px;padding:8px 14px;border-radius:var(--border-radius-lg);border:0.5px solid var(--color-border-tertiary);background:var(--color-background-primary);font-size:13px;font-weight:500;color:var(--color-text-primary)}
.badge img{width:20px;height:20px;object-fit:contain}
.group-label{font-size:11px;font-weight:500;color:var(--color-text-secondary);text-transform:uppercase;letter-spacing:.06em;width:100%;margin:8px 0 2px}
</style>

<div style="padding:4px 0">

<div class="group-label">Orchestration</div>
<div class="stack-wrap">
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apacheairflow/apacheairflow-original.svg" alt="Airflow logo"><span>Apache Airflow</span></div>
</div>

<div class="group-label">Warehouse & Storage</div>
<div class="stack-wrap">
  <div class="badge"><img src="img/BigQuery-512-color.svg" alt="GCP logo"><span>BigQuery</span></div>
  <div class="badge"><img src="img/Cloud_Storage-512-color.svg" alt="GCP logo"><span>Cloud Storage</span></div>
</div>

<div class="group-label">Transformation & Quality</div>
<div class="stack-wrap">
  <div class="badge"><img src="img/Dbt--Streamline-Svg-Logos.svg" alt="dbt logo"><span>dbt</span></div>
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/pytest/pytest-original.svg" alt="pytest logo"><span>pytest</span></div>
</div>

<div class="group-label">Infrastructure & DevOps</div>
<div class="stack-wrap">
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/terraform/terraform-original.svg" alt="Terraform logo"><span>Terraform</span></div>
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" alt="Docker logo"><span>Docker</span></div>
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/githubactions/githubactions-original.svg" alt="GitHub logo"><span>GitHub Actions</span></div>
</div>

<div class="group-label">Alerting & Visualisation</div>
<div class="stack-wrap">
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/slack/slack-original.svg" alt="Slack logo"><span>Slack</span></div>
  <div class="badge"><img src="img/Looker-512-color.svg" alt="GCP logo"><span>Looker Studio</span></div>
</div>

<div class="group-label">Languages</div>
<div class="stack-wrap">
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python logo"><span>Python</span></div>
  <div class="badge"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" alt="SQL logo"><span>SQL</span></div>
</div>

</div>

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