from airflow.sdk import dag, task

@dag(dag_id = "first_task",)
def first_dag():

    @task.python
    def first_task():
        print("This is the first task. Hello, Airflow!")
    
    @task.python
    def second_task():
        print("This is the second task. Hello, Airflow!")
    
    @task.python
    def third_task():
        print("This is the third task. Hello, Airflow!")

    first = first_task()
    second = second_task()
    third = third_task()

    # Set task dependencies
    first >> second >> third

# Instantiate the DAG
first_dag()
    