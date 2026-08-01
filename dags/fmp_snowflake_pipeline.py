import os
import tempfile
from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "You",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="airflow_dag_fmp_data",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fmp", "snowflake"],
)
def fmp_pipeline():

    @task()
    def get_sp500_symbols() -> list:
        """Task 1: Fetch S&P 500 symbols from Wikipedia with proper User-Agent header."""
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers)
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

        # Write to a temp file and use SnowflakeHook with verified snowflake_default connection
        with tempfile.TemporaryDirectory() as tmpdir:
            local_file_path = os.path.join(tmpdir, file_name)
            df.to_csv(local_file_path, index=False)

            snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
            conn = snowflake_hook.get_conn()
            cursor = conn.cursor()

            put_sql = f"PUT file://{local_file_path} @FINANCIAL_DB.RAW.SP500_STAGE AUTO_COMPRESS=FALSE;"
            cursor.execute(put_sql)

            cursor.close()
            conn.close()

        print(f"Successfully uploaded {file_name} to Snowflake Stage @SP500_STAGE.")

    @task()
    def hit_fmp_api(symbols: list) -> list:
        """Task 2 (Branch B): Hit FMP API for company profiles."""
        test_symbols = symbols[:2]
        api_key = Variable.get(
            "fmp_api_key", default_var="TGqnPdIo3fJ05arOQYKE7BdpGrosOxpj"
        )
        profiles = []

        for symbol in test_symbols:
            url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={api_key}"
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

        insert_sql = """
        INSERT INTO FINANCIAL_DB.RAW.FMP_COMPANY_PROFILES (
            SYMBOL, PRICE, BETA, VOL_AVG, MARKET_CAP, 
            COMPANY_NAME, EXCHANGE, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR, COUNTRY
        ) VALUES (
            %(symbol)s, %(price)s, %(beta)s, %(volAvg)s, %(mktCap)s,
            %(companyName)s, %(exchangeShortName)s, %(industry)s, %(website)s, 
            %(description)s, %(ceo)s, %(sector)s, %(country)s
        );
        """

        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        for profile in profiles:
            # Safe parsing prevents KeyError issues with variable API payloads
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
            cursor.execute(insert_sql, safe_profile)

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
