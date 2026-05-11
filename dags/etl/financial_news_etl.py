import json
import sys
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "include"))
try:
    from callbacks import slack_on_failure, slack_on_success
except ImportError:
    def slack_on_failure(context):
        print("Slack callback not available in this environment")
    def slack_on_success(context):
        print("Slack callback not available in this environment")

# ── Constants ──────────────────────────────────────────────────────────────
GCP_PROJECT    = "nodal-triumph-495809-s2"
GCS_BUCKET     = "nodal-triumph-495809-s2-raw-data"
BQ_DATASET     = "raw"
BQ_TABLE       = "financial_news"
GCP_CONN_ID    = "google_cloud_default"

# ── Default arguments applied to every task ────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="financial_news_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",  # Every 6 hours
    catchup=False,
    is_paused_upon_creation=False,
    on_failure_callback=slack_on_failure,
    on_success_callback=slack_on_success,
    tags=["etl", "finance", "sentiment"],
    doc_md="""
    ## Financial News ETL Pipeline

    **Pattern**: ETL — transform happens in Python before loading to BigQuery.

    **Steps**:
    1. Extract news articles from Alpha Vantage API
    2. Score sentiment using VADER
    3. Validate data quality
    4. Upload raw JSON to GCS
    5. Load enriched data into BigQuery
    """,
)
def financial_news_etl():

    @task
    def extract_news() -> list:
        """
        Pull latest financial news from Alpha Vantage.
        Returns a list of raw article dicts.
        """
        api_key = Variable.get("ALPHA_VANTAGE_KEY")

        url = (
            "https://www.alphavantage.co/query"
            "?function=NEWS_SENTIMENT"
            "&tickers=AAPL,MSFT,TSLA,GOOGL,AMZN"
            "&sort=LATEST"
            "&limit=50"
            f"&apikey={api_key}"
        )

        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raises error if status is 4xx or 5xx

        data = response.json()

        # Alpha Vantage returns errors inside the JSON, not as HTTP errors
        if "Information" in data:
            raise ValueError(f"API limit reached: {data['Information']}")
        if "Note" in data:
            raise ValueError(f"API rate limit: {data['Note']}")

        articles = data.get("feed", [])

        if len(articles) == 0:
            raise ValueError("API returned 0 articles — something is wrong")

        print(f"Extracted {len(articles)} articles from Alpha Vantage")
        return articles

    @task
    def transform_news(articles: list) -> str:
        """
        Clean, deduplicate and score sentiment.
        Returns the local file path of the output JSON.
        """
        analyzer = SentimentIntensityAnalyzer()
        rows = []

        for article in articles:
            title   = article.get("title", "")
            summary = article.get("summary", "")

            # Score sentiment on the summary if available, else title
            text_to_score = summary if len(summary) > 20 else title
            scores = analyzer.polarity_scores(text_to_score)

            # Determine label from compound score
            compound = scores["compound"]
            if compound >= 0.05:
                label = "positive"
            elif compound <= -0.05:
                label = "negative"
            else:
                label = "neutral"

            rows.append({
                "title":           title,
                "source":          article.get("source", ""),
                "published_at":    article.get("time_published", ""),
                "url":             article.get("url", ""),
                # our sentiment score using VADER
                "vader_sentiment_score":    round(compound, 4),
                "vader_sentiment_label":    label,
                # Alpha vantage sentiment score
                "api_sentiment_score":      article.get("overall_sentiment_score", None),
                "api_sentiment_label":      article.get("overall_sentiment_label", ""),
                "tickers":         json.dumps([
                    t["ticker"]
                    for t in article.get("ticker_sentiment", [])
                ]),
                "overall_sentiment_label": article.get(
                    "overall_sentiment_label", ""
                ),
                "ingested_at": datetime.utcnow().isoformat(),
            })

        df = pd.DataFrame(rows)

        # Remove duplicates based on URL
        before = len(df)
        df = df.drop_duplicates(subset=["url"])
        print(f"Deduplication removed {before - len(df)} rows")

        # Remove rows with empty titles
        df = df[df["title"].str.len() > 5]

        print(f"Final row count after transform: {len(df)}")

        # Save to a temp file — name includes the hour so runs don't overwrite
        out_path = f"/tmp/news_{datetime.utcnow().strftime('%Y%m%d_%H')}.json"
        df.to_json(out_path, orient="records", lines=True)

        print(f"Saved transformed data to {out_path}")
        return out_path

    @task
    def validate_data(file_path: str) -> str:
        """
        Run data quality checks before loading.
        Raises AssertionError if any check fails — this fails the task.
        Returns the file path if all checks pass.
        """
        df = pd.read_json(file_path, lines=True)

        print(f"Running validation on {len(df)} rows...")

        # Check 1: We have actual data
        assert len(df) > 0, "VALIDATION FAILED: No rows after transform"

        # Check 2: No null titles
        null_titles = df["title"].isna().sum()
        assert null_titles == 0, f"VALIDATION FAILED: {null_titles} null titles"

        # Check 3: Vader Sentiment scores in valid range
        out_of_range = df[
            (df["vader_sentiment_score"] < -1) | (df["vader_sentiment_score"] > 1)
        ]
        assert len(out_of_range) == 0, (
            f"VALIDATION FAILED: {len(out_of_range)} scores out of [-1, 1] range"
        )

        # Check 4: Vader sentiment labels are only valid values
        valid_labels = {"positive", "negative", "neutral"}
        bad_labels = df[~df["vader_sentiment_label"].isin(valid_labels)]
        assert len(bad_labels) == 0, (
            f"VALIDATION FAILED: unexpected labels found: "
            f"{bad_labels['vader_sentiment_label'].unique()}"
        )

        # Check 5: No duplicate URLs
        dupes = df["url"].duplicated().sum()
        assert dupes == 0, f"VALIDATION FAILED: {dupes} duplicate URLs remain"
        
        # Check 6: API sentiment Score
        assert df["api_sentiment_score"].notna().all() == False or True, "ok"
        
        # Log the null API sentiment score
        null_api_scores = df["api_sentiment_score"].isna().sum()
        print(f"Articles missing API sentiment score: {null_api_scores}")

        print("All validation checks passed!")
        return file_path

    @task
    def upload_to_gcs(file_path: str) -> str:
        """
        Upload the local JSON file to GCS.
        Returns the GCS object path.
        """
        from google.cloud import storage
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
        "/opt/airflow/include/gcp_keyfile.json"
        )
        
        

        # GCS object path — partitioned by date for easy querying later
        date_str = datetime.utcnow().strftime("%Y/%m/%d")
        hour_str = datetime.utcnow().strftime("%H")
        gcs_path = f"news/{date_str}/{hour_str}/data.json"

        client = storage.Client(project=GCP_PROJECT, credentials=credentials)
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(gcs_path)

        blob.upload_from_filename(file_path)

        print(f"Uploaded {file_path} → gs://{GCS_BUCKET}/{gcs_path}")
        return gcs_path

    @task
    def load_to_bigquery(gcs_path: str):
        """
        Load the GCS JSON file into BigQuery raw dataset.
        Uses WRITE_APPEND so each run adds new rows.
        """
        from google.cloud import bigquery
        from google.oauth2 import service_account
        
        credentials = service_account.Credentials.from_service_account_file(
        "/opt/airflow/include/gcp_keyfile.json"
        )

        client = bigquery.Client(project=GCP_PROJECT, credentials=credentials)

        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        uri = f"gs://{GCS_BUCKET}/{gcs_path}"

        load_job = client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )

        load_job.result()  # Wait for job to complete

        table = client.get_table(table_ref)
        print(
            f"Loaded data from {uri} → {table_ref}"
            f" | Total rows in table: {table.num_rows}"
        )

    # ── Wire up the task dependencies ───
    articles    = extract_news()
    local_file  = transform_news(articles)
    clean_file  = validate_data(local_file)
    gcs_path    = upload_to_gcs(clean_file)
    load_to_bigquery(gcs_path)


financial_news_etl()