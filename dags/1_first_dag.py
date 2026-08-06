from airflow.sdk import dag, task

<<<<<<< HEAD
@dag(dag_id = "first_task",)
=======

@dag(dag_id="first_task")
>>>>>>> 336b826bd5432c59fadae31463e29bddabbbf431
def first_dag():

    @task.python
    def first_task():
<<<<<<< HEAD
        print("This is the first task. Hello, Airflow!")
    
    @task.python
    def second_task():
        print("This is the second task. Hello, Airflow!")
    
    @task.python
    def third_task():
        print("This is the third task. Hello, Airflow!")
=======
        print("This is my first task")

    @task.python
    def second_task():
        print("This is my second task")

    @task.python
    def third_task():
        print("This is my third task")
>>>>>>> 336b826bd5432c59fadae31463e29bddabbbf431

    first = first_task()
    second = second_task()
    third = third_task()

<<<<<<< HEAD
    # Set task dependencies
    first >> second >> third

# Instantiate the DAG
first_dag()
    
=======
    first >> second >> third


first_dag()
>>>>>>> 336b826bd5432c59fadae31463e29bddabbbf431
