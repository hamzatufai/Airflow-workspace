from airflow.sdk import dag, task

@dag(dag_id="xcom_auto_dag",)
def xcom_auto_dag():

    @task.python
    def first_task():
        print("Extracting data... This is my first task")
        fetch_data = {"data": [1, 2, 3, 4, 5]}
        return fetch_data  # This will be automatically pushed to XComs

    @task.python
    def second_task(data: dict):
        fetch_data = data["data"]
        fetch_data = fetch_data*2  # Example transformation
        transformed_data = {"trans_data": fetch_data}
        return transformed_data

    @task.python
    def third_task(data: dict):
        loaded_data = data["trans_data"]
        print(f"Loading data... This is my third task: {loaded_data}")
        return "Data loaded successfully!"  # This will be automatically pushed to XComs

    # Define the task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)

# Instantiate the DAG
xcom_auto_dag()
