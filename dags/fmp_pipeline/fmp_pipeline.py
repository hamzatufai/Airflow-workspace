import os
import sys
from datetime import datetime
import pandas as pd
import requests

# Ensure package subfolder is added to sys.path for local module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Config Imports
from config import (
    DEFAULT_ARGS,
    AWS_CONN_ID,
    SNOWFLAKE_CONN_ID,
    S3_BUCKET_NAME,
    S3_KEY_PREFIX,
    WIKIPEDIA_URL,
    FMP_API_BASE_URL,
    HTTP_HEADERS,
    INSERT_FMP_PROFILES_SQL,
)


@dag(
    dag_id="airflow_dag_fmp_data",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fmp", "aws", "s3", "snowflake"],
)
def fmp_pipeline():

    @task()
    def get_sp500_symbols() -> list:
        """Task 1: Fetch S&P 500 symbols dynamically."""
        response = requests.get(WIKIPEDIA_URL, headers=HTTP_HEADERS)
        response.raise_for_status()

        tables = pd.read_html(response.text)
        df = tables[0]
        return df["Symbol"].str.replace(".", "-").tolist()

    @task()
    def save_file_into_s3(symbols: list):
        """Task 2 (Branch A): Upload CSV into AWS S3 Bucket using S3Hook."""
        df = pd.DataFrame(symbols, columns=["symbol"])
        csv_buffer = df.to_csv(index=False)

        # Dynamic naming and S3 key generation
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"sp500_symbols_{timestamp}.csv"
        s3_key = f"{S3_KEY_PREFIX}/{file_name}"

        # Get bucket name from Airflow Variables or config default
        bucket_name = Variable.get("s3_bucket_name", default_var=S3_BUCKET_NAME)

        # Upload string buffer directly to S3 via S3Hook
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=csv_buffer,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True,
        )

        print(
            f"Successfully uploaded {file_name} to S3 bucket s3://{bucket_name}/{s3_key}"
        )

    @task()
    def hit_fmp_api(symbols: list) -> list:
        """Task 2 (Branch B): Hit FMP API for company profiles."""
        test_symbols = symbols[:2]
        api_key = Variable.get(
            "fmp_api_key", default_var="TGqnPdIo3fJ05arOQYKE7BdpGrosOxpj"
        )
        profiles = []

        for symbol in test_symbols:
            url = f"{FMP_API_BASE_URL}?symbol={symbol}&apikey={api_key}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, list):
                    profiles.append(data[0])

        return profiles

    @task()
    def load_data_snowflake(profiles: list):
        """Task 3: Safely load API profiles into Snowflake Table."""
        if not profiles:
            print("No profiles fetched.")
            return

        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        for profile in profiles:
            safe_profile = {
                "symbol": profile.get("symbol"),
                "price": profile.get("price"),
                "beta": profile.get("beta"),
                "volAvg": profile.get("volAvg") or profile.get("volAvg", None),
                "mktCap": profile.get("mktCap") or profile.get("marketCap", None),
                "companyName": profile.get("companyName"),
                "exchangeShortName": profile.get("exchangeShortName")
                or profile.get("exchange", None),
                "industry": profile.get("industry"),
                "website": profile.get("website"),
                "description": profile.get("description"),
                "ceo": profile.get("ceo"),
                "sector": profile.get("sector"),
                "country": profile.get("country"),
            }
            cursor.execute(INSERT_FMP_PROFILES_SQL, safe_profile)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully loaded {len(profiles)} records into Snowflake.")

    # Execution Flow strictly matches your Excalidraw diagram
    symbols = get_sp500_symbols()
    save_file_into_s3(symbols)
    fmp_data = hit_fmp_api(symbols)
    load_data_snowflake(fmp_data)


dag_instance = fmp_pipeline()
