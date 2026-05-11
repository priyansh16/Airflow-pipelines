"""
Airflow Callbacks
==================
Reusable callback functions for DAG-level event handling.
Import these into any DAG that needs alerting.
"""
import requests
from airflow.models import Variable


def slack_on_failure(context):
    """
    Send a Slack alert when any task in a DAG fails.
    
    Usage in a DAG:
        from include.callbacks import slack_on_failure
        
        @dag(on_failure_callback=slack_on_failure, ...)
        def my_dag(): ...
    """
    try:
        webhook_url = Variable.get("SLACK_WEBHOOK_URL")
    except Exception:
        print("SLACK_WEBHOOK_URL not configured — skipping Slack alert")
        return

    dag_id   = context["dag"].dag_id
    task_id  = context["task"].task_id
    exec_dt  = context["logical_date"]
    log_url  = context["task_instance"].log_url

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Pipeline Failure Alert"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Execution date:*\n{exec_dt}"},
                    {"type": "mrkdwn", "text": f"*Logs:*\n<{log_url}|View logs>"},
                ]
            }
        ]
    }

    response = requests.post(webhook_url, json=message, timeout=10)

    if response.status_code != 200:
        print(
            f"Slack alert failed: {response.status_code} {response.text}"
        )
    else:
        print(f"Slack alert sent for {dag_id}.{task_id}")


def slack_on_success(context):
    """
    Send a Slack alert when a DAG completes successfully.
    Fires at the DAG level, not the task level.
    """
    try:
        webhook_url = Variable.get("SLACK_WEBHOOK_URL")
    except Exception:
        print("SLACK_WEBHOOK_URL not configured — skipping Slack alert")
        return

    dag_id  = context["dag"].dag_id
    exec_dt = context["logical_date"]
    run_id  = context["run_id"]

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Pipeline Succeeded"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Status:*\nsuccess"},
                    {"type": "mrkdwn", "text": f"*Execution date:*\n{exec_dt}"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{run_id}"},
                ]
            }
        ]
    }

    response = requests.post(webhook_url, json=message, timeout=10)

    if response.status_code != 200:
        print(f"Slack alert failed: {response.status_code} {response.text}")
    else:
        print(f"Slack success alert sent for {dag_id}")