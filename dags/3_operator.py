from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator


@dag(dag_id="operators_task")
def operators_dag():

    @task.python
    def first_task():
        print("This is my first task")

    @task.python
    def second_task():
        print("This is my second task")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org"

    bash_task_oldschool = BashOperator(
        task_id="bash_task_oldshool",
        bash_command="echo https://airflow.apache.org",
    )

    first = first_task()
    second = second_task()
    bash_task_modern = bash_task_modern()
    bash_task_oldschool = bash_task_oldschool

    # Set task dependencies
    first >> second >> bash_task_modern >> bash_task_oldschool


# Instantiate the DAG
operators_dag()
