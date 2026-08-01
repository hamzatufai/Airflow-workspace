from airflow.sdk import dag, task


@dag(dag_id="xcom_dags_auto",)
def xcom_dags_auto():

    @task.python
    def first_task():
        print("Extracting data... This is the first task")
        fetch_data = {"data": [1, 2, 3, 4, 5]}
        return fetch_data

    @task.python
    def second_task(data: dict):
        fetched_data = data["data"]
        transformed_data = fetched_data*2
        transformed_data_dict = {"trans_data": transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data: dict):
        load_data = data
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

xcom_dags_auto()
