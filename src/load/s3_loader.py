import pandas as pd
import boto3
from io import StringIO
from utils.logger import logger

S3_BUCKET = "etl-financial-data"
AWS_REGION = "sa-east-1"

S3_KEYS = {
    "dim_company": "processed/dimensions/dim_company/dim_company.csv",
    "dim_account": "processed/dimensions/dim_account/dim_account.csv",
    "fact_balance": "processed/facts/fact_balance/fact_balance.csv",
    "wide_table": "processed/wide/wide_table.csv",
    "financial_indicators": "analytics/financial_indicators.csv",
    "financial_evolution": "analytics/financial_evolution.csv",
}


def upload_csv_s3(df: pd.DataFrame, bucket: str, key: str, s3_client):
    buffer = StringIO()
    df.to_csv(buffer, index=False)

    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def run_load_s3(datasets: dict):

    s3 = boto3.client("s3", region_name=AWS_REGION)

    for name, df in datasets.items():
        if name not in S3_KEYS:
            logger.warning(f"S3: dataset sem key mapeada, ignorando: {name}")
            continue

        key = S3_KEYS[name]
        upload_csv_s3(df, S3_BUCKET, key, s3)

        logger.info(f"S3 OK: s3://{S3_BUCKET}/{key}")
