# Airflow-workspace
<img width="1599" height="899" alt="WhatsApp Image 2026-07-26 at 9 44 56 PM" src="https://github.com/user-attachments/assets/2cd6996d-5c39-491a-b827-7873c013ba9b" />

================================================================================
                    APACHE AIRFLOW: THE ULTIMATE DEEP DIVE
================================================================================

1. WHAT IS APACHE AIRFLOW?
--------------------------------------------------------------------------------
Analogy:
Imagine running a busy restaurant kitchen. Every morning, strict steps must happen 
in a precise order:
  1. Unlock the kitchen doors.
  2. Turn on the gas and oven.
  3. Clean the food prep tables.
  4. Prep the ingredients for cooking.

If Step 2 fails (the gas won't turn on), you CANNOT prep food or start cooking! 
Apache Airflow acts as the automated Head Chef. It ensures steps happen in exact 
sequence, retries steps if they fail, notifies you on Slack/Email when something 
 breaks, and prevents tasks from running out of order.

Technical Definition:
Apache Airflow is an open-source workflow orchestration platform used to 
programmatically author, schedule, and monitor data pipelines.

What Airflow IS:
- An Orchestration Framework: The conductor of an orchestra. It tells other tools 
  WHEN to run, WHAT to run, and WHERE to push results.

What Airflow IS NOT:
- NOT a Data Streaming Framework: It does not process real-time, event-driven data 
  streams millisecond-by-millisecond (like Apache Kafka, Apache Flink, or Spark Streaming).
- NOT a Heavy Data Processing Engine: It does not do the heavy lifting of processing 
  gigabytes or terabytes of data inside its own execution layer (like Apache Spark, 
  Trino, or Snowflake). It simply triggers those engines.


2. LINEAR VS. COMPLEX (BRANCHING) WORKFLOWS
--------------------------------------------------------------------------------
Linear Flow:
Tasks execute sequentially, one after another.

  [Task A: Extract API Data]
             │
             ▼
  [Task B: Transform CSV File]
             │
             ▼
  [Task C: Load into Snowflake]

Complex / Branching Flow:
Tasks run in parallel or dynamically branch based on conditions.

<img width="721" height="159" alt="Screenshot 2026-08-05 233913" src="https://github.com/user-attachments/assets/31919b2e-59be-4026-8ec8-05bd4c4bbb3a" />


3. CORE ARCHITECTURE & COMPONENTS
--------------------------------------------------------------------------------
Airflow runs as a distributed system made of several decoupled services:

1. Metadata Database (Airflow DB):
   - Analogy: The Central Ledger / Brain.
   - Function: Uses PostgreSQL or MySQL to store all system states, task histories, 
     credentials, variables, user permissions, and DAG definitions.

2. DAG File Processor:
   - Analogy: The Code Reader.
   - Function: Continuously scans the dags/ folder, executes Python code to parse 
     DAG structures, and syncs updated definitions to the Metadata DB.

3. Airflow Webserver (UI):
   - Analogy: The Mission Control Center.
   - Function: A Flask web application providing a user interface to view DAG execution 
     graphs, inspect logs, trigger manual runs, and debug failures.

4. Scheduler:
   - Analogy: The Logistics Manager.
   - Function: Monitors the Metadata DB, determines which tasks are ready to run based 
     on dependencies and schedule constraints, and hands them to the Executor.

5. Executor:
   - Analogy: The Work Dispatcher.
   - Function: Defines HOW tasks get assigned to computing resources. (e.g., LocalExecutor, 
     CeleryExecutor for distributed tasks, or KubernetesExecutor for container pods).

6. Workers:
   - Analogy: The Factory Laborers.
   - Function: The actual processes/machines executing the inner code of tasks.

7. Triggerer:
   - Analogy: The Async Watchman.
   - Function: Runs an asynchronous event loop to handle deferred tasks (like waiting for an S3 file) 
     without keeping a worker thread occupied.


4. KEY BUILDING BLOCKS: DAGs, TASKS, OPERATORS, SENSORS
--------------------------------------------------------------------------------
DAG (Directed Acyclic Graph):
- Directed: Data and execution flow strictly forward in one direction.
- Acyclic: No infinite loops allowed (e.g., Task A -> Task B -> Task A is invalid).
- Graph: A network of interconnected task nodes.

Task:
- A single node in a DAG representing an individual unit of work (e.g., "Run SQL Query").

Operator:
- The template/blueprint defining what a task actually does.
  - PythonOperator: Executes a Python function.
  - BashOperator: Runs shell commands.
  - PostgresOperator / SnowflakeOperator: Executes SQL scripts on remote databases.

Sensor:
- A specialized operator designed to continuously poll/wait for an external event 
  (e.g., waiting for an S3 file arrival or a web API HTTP 200 response) before unlocking downstream tasks.


5. COMPLETE DAG EXECUTION LIFECYCLE
--------------------------------------------------------------------------------
1. Authoring: Developer writes a Python file in `dags/` specifying DAG dependencies.
2. Parsing: DAG File Processor parses the file and registers the DAG graph into the Database.
3. Scheduling: Scheduler evaluates execution schedules. When a run is due, it creates a 
   `DagRun` entry in the DB and marks initial tasks as `SCHEDULED`.
4. Queuing: Scheduler pushes `SCHEDULED` tasks to the Executor queue (marking state as `QUEUED`).
5. Execution: Executor pulls tasks from queue, hands them to an available Worker process, 
   setting task status to `RUNNING`.
6. Completion: Worker finishes execution and updates the DB state to `SUCCESS` or `FAILED`.
7. Reporting: Webserver fetches updated states from DB and visually renders status colors on the UI dashboard.


6. SCHEDULING, LOGICAL DATES, AND JINJA TEMPLATING
--------------------------------------------------------------------------------
Scheduling Parameters:
- `start_date`: The timestamp at which DAG scheduling begins backfilling/running.
- `schedule`: Interval controlling run frequency (e.g., `@daily`, `0 0 * * *`, `@hourly`).
- `end_date`: Optional cutoff timestamp after which scheduling ceases.

Logical Date vs. Actual Run Date:
- Logical Date (`data_interval_start` / execution_date): Represents the *start* of the period 
  being processed (e.g., data window for 2026-08-01).
- Actual Run Date (`data_interval_end`): The exact time the interval finishes and execution triggers 
  (e.g., a daily schedule for 2026-08-01 executes at 2026-08-02 00:00:00).

Jinja Templating:
Airflow enables dynamic values during execution using Jinja tags (`{{ }}`):

```python
run_etl = BashOperator(
    task_id="process_daily_data",
    bash_command="python /scripts/process.py --date {{ ds }}",  # {{ ds }} renders as YYYY-MM-DD
)


ADVANCED DEEP DIVE: XCOMS, KWARGS, AND QUEUES

XComs (Cross-Communications):

Purpose: A mechanism allowing tasks to pass small amounts of metadata to each other.

Storage: Saved directly in the Airflow Metadata Database.

A. Manual XComs:
Explicitly pushing/pulling key-value pairs via TaskInstance contexts:

Push: kwargs['ti'].xcom_push(key='file_count', value=42)

Pull: kwargs['ti'].xcom_pull(task_ids='previous_task', key='file_count')

B. Automatic XComs:

Any return value from a Python function in a PythonOperator automatically
gets stored in XCom under the key return_value.

TaskFlow API (@task decorator): Automatically manages XCom dependencies behind
the scenes without explicit push/pull function calls.

kwargs (Context Dictionary):

In Python, **kwargs catches additional key-value arguments.

In Airflow tasks, **kwargs allows your python function to access Airflow's internal execution
context (e.g., kwargs['ds'] for execution date, kwargs['ti'] for TaskInstance object, kwargs['dag']).

Queues:

In distributed executor environments (e.g., CeleryExecutor), queues direct specific tasks to
isolated worker pools.

Example: Assigning compute-heavy tasks to GPU workers, while keeping lightweight tasks on default workers.

Python
ml_task = PythonOperator(
    task_id="train_model",
    python_callable=train_gpu_model,
    queue="gpu_worker_queue"  # Picks up tasks exclusively on workers listening to this queue
)
EXECUTIVE SUMMARY / ARCHITECTURE CHEAT SHEET

+-------------------+
|  Python DAG File  |
+---------+---------+
        |
        v
+-------------------+      +----------------------+      +----------------------+
| DAG FileProcessor | ---> |  Metadata Database   | <--- |   Airflow Web UI     |
+-------------------+      +----------+-----------+      +----------------------+
        |
        v
+----------------------+
|   Airflow Scheduler  |
+----------+-----------+
        |
        v
+----------------------+
|   Airflow Executor   |
+----------+-----------+
        |
        v
+----------------------+
|   Airflow Workers    |
+----------------------+

