import os
import tempfile
from datetime import datetime
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Import configurations from config.py
from config import (
    DEFAULT_ARGS,
    SNOWFLAKE_CONN_ID,
    SNOWFLAKE_STAGE,
    WIKIPEDIA_URL,
    FMP_API_BASE_URL,
    DEFAULT_FMP_API_KEY,
    HTTP_HEADERS,
    INSERT_FMP_PROFILES_SQL,
)


@dag(
    dag_id="airflow_dag_fmp_data",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fmp", "snowflake", "production"],
)
def fmp_pipeline():

    @task()
    def get_sp500_symbols() -> list:
        """Task 1: Fetch S&P 500 symbols from Wikipedia."""
        response = requests.get(WIKIPEDIA_URL, headers=HTTP_HEADERS)
        response.raise_for_status()

        tables = pd.read_html(response.text)
        df = tables[0]
        symbols = df["Symbol"].str.replace(".", "-").tolist()
        return symbols

    @task()
    def save_file_into_s3(symbols: list):
        """Task 2 (Branch A): Upload CSV into Snowflake Stage via PUT command."""
        df = pd.DataFrame(symbols, columns=["symbol"])
        file_name = f"sp500_symbols_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file_path = os.path.join(tmpdir, file_name)
            df.to_csv(local_file_path, index=False)

            snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            conn = snowflake_hook.get_conn()
            cursor = conn.cursor()

            put_sql = (
                f"PUT file://{local_file_path} {SNOWFLAKE_STAGE} AUTO_COMPRESS=FALSE;"
            )
            cursor.execute(put_sql)

            cursor.close()
            conn.close()

        print(
            f"Successfully uploaded {file_name} to Snowflake Stage {SNOWFLAKE_STAGE}."
        )

    @task()
    def hit_fmp_api(symbols: list) -> list:
        """Task 2 (Branch B): Hit FMP API for company profiles."""
        test_symbols = symbols[:2]
        api_key = Variable.get("fmp_api_key", default_var=DEFAULT_FMP_API_KEY)
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

    # Execution Flow
    symbols = get_sp500_symbols()
    save_file_into_s3(symbols)
    fmp_data = hit_fmp_api(symbols)
    load_data_snowflake(fmp_data)


dag_instance = fmp_pipeline()
