from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"]
)
def hello_world_dag():

    @task
    def say_hello():
        print("Airflow is working!")
        return "Hello from Airflow"

    @task
    def say_goodbye(message: str):
        print(f"Received: {message}")
        print("Goodbye from Airflow!")

    message = say_hello()
    say_goodbye(message)

hello_world_dag()