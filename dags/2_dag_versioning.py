from airflow.sdk import dag, task


@dag(dag_id="versioning_dag")
def versioning_dag():

    @task.python
    def first_task():
        print("This is my first task")

    @task.python
    def second_task():
        print("This is my second task")

    @task.python
    def third_task():
        print("This is my third task")

    @task.python
    def version_task():
        print("This is my version task. 2.0")

    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()

    # Set task dependencies
    first >> second >> third >> version

# Instantiate the DAG
versioning_dag()
