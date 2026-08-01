from airflow.sdk import dag, task


@dag(dag_id="first_task")
def first_dag():

    @task.python
    def first_task():
        print("This is my first task")

    @task.python
    def second_task():
        print("This is my second task")

    @task.python
    def third_task():
        print("This is my third task")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


first_dag()
