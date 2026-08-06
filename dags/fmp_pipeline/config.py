import os
from datetime import timedelta

# Default DAG configuration
DEFAULT_ARGS = {
    "owner": "You",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Airflow Connection IDs
AWS_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

# S3 & Snowflake Target Defaults
S3_BUCKET_NAME = "my-financial-data-bucket"  # Default fallback
S3_KEY_PREFIX = "sp500_symbols"  # S3 folder path
SNOWFLAKE_TABLE = "FINANCIAL_DB.RAW.FMP_COMPANY_PROFILES"

# API & Web Endpoints
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
FMP_API_BASE_URL = "https://financialmodelingprep.com/stable/profile"

# HTTP Headers
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# SQL Template
INSERT_FMP_PROFILES_SQL = f"""
INSERT INTO {SNOWFLAKE_TABLE} (
    SYMBOL, PRICE, BETA, VOL_AVG, MARKET_CAP, 
    COMPANY_NAME, EXCHANGE, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR, COUNTRY
) VALUES (
    %(symbol)s, %(price)s, %(beta)s, %(volAvg)s, %(mktCap)s,
    %(companyName)s, %(exchangeShortName)s, %(industry)s, %(website)s, 
    %(description)s, %(ceo)s, %(sector)s, %(country)s
);
"""
