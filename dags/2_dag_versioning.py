from airflow.sdk import dag, task


@dag(
    dag_id="versioning_task",
)
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

    @task.python
    def versioning_task():
        print(
            "This is the versioning task. Now  you see the changes in the DAG versioning!"
        )

    @task.python
    def multiply_by_2_task():
        my_list = [1, 2, 3, 4, 5]
        multiplied_list = [x * 2 for x in my_list]
        return multiplied_list
       

    first = first_task()
    second = second_task()
    third = third_task()
    versioning = versioning_task()
    multiplied = multiply_by_2_task() 

    # Define the task dependencies
    first >> second >> third >> versioning >> multiplied
# Instantiate the DAG
first_dag()
