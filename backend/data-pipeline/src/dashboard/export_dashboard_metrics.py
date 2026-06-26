import os
import time
import urllib.parse

import boto3


athena = boto3.client("athena")
s3 = boto3.client("s3")


BUCKET_NAME = os.environ["BUCKET_NAME"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
GLUE_CATALOG_NAME = os.environ["GLUE_CATALOG_NAME"]
GOLD_TABLE_NAME = os.environ["GOLD_TABLE_NAME"]
WORKGROUP_NAME = os.environ["WORKGROUP_NAME"]
DESTINATION_KEY = os.environ["DESTINATION_KEY"]


def parse_s3_uri(s3_uri: str):
    parsed = urllib.parse.urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def lambda_handler(event, context):
    query = f"""
    SELECT
      department,
      admission_type,
      sepsis_risk_category,
      SUM(total_records) AS total_records,
      SUM(abnormal_vitals_count) AS abnormal_vitals_count,
      SUM(deterioration_next_12h_count) AS deterioration_next_12h_count,
      SUM(nurse_alert_count) AS nurse_alert_count,
      SUM(high_sepsis_risk_count) AS high_sepsis_risk_count,
      ROUND(AVG(abnormal_vitals_rate), 4) AS abnormal_vitals_rate,
      ROUND(AVG(deterioration_next_12h_rate), 4) AS deterioration_next_12h_rate,
      ROUND(AVG(nurse_alert_rate), 4) AS nurse_alert_rate,
      ROUND(AVG(high_sepsis_risk_rate), 4) AS high_sepsis_risk_rate,
      ROUND(AVG(avg_heart_rate), 2) AS avg_heart_rate,
      ROUND(AVG(avg_respiratory_rate), 2) AS avg_respiratory_rate,
      ROUND(AVG(avg_spo2_pct), 2) AS avg_spo2_pct,
      ROUND(AVG(avg_temperature_c), 2) AS avg_temperature_c,
      ROUND(AVG(avg_sepsis_risk_score), 4) AS avg_sepsis_risk_score
    FROM {DATABASE_NAME}.{GOLD_TABLE_NAME}
    GROUP BY
      department,
      admission_type,
      sepsis_risk_category
    ORDER BY
      department,
      admission_type,
      sepsis_risk_category
    """

    print("Starting Athena dashboard export query")
    print(f"Glue Catalog: {GLUE_CATALOG_NAME}")
    print(f"Database: {DATABASE_NAME}")
    print(f"Gold table: {GOLD_TABLE_NAME}")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Catalog": GLUE_CATALOG_NAME,
            "Database": DATABASE_NAME,
        },
        WorkGroup=WORKGROUP_NAME,
    )

    query_execution_id = response["QueryExecutionId"]
    print(f"QueryExecutionId: {query_execution_id}")

    final_state = None
    output_location = None

    for _ in range(80):
        execution = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = execution["QueryExecution"]["Status"]
        final_state = status["State"]

        if final_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            if final_state == "SUCCEEDED":
                output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
            break

        time.sleep(5)

    if final_state != "SUCCEEDED":
        reason = status.get("StateChangeReason", "Unknown reason")
        raise Exception(f"Athena dashboard export failed. State={final_state}. Reason={reason}")

    source_bucket, source_key = parse_s3_uri(output_location)

    print(f"Athena output: s3://{source_bucket}/{source_key}")
    print(f"Copying dashboard CSV to: s3://{BUCKET_NAME}/{DESTINATION_KEY}")

    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={
            "Bucket": source_bucket,
            "Key": source_key,
        },
        Key=DESTINATION_KEY,
    )

    return {
        "status": "dashboard_metrics_exported",
        "glue_catalog": GLUE_CATALOG_NAME,
        "database": DATABASE_NAME,
        "gold_table": GOLD_TABLE_NAME,
        "query_execution_id": query_execution_id,
        "source": f"s3://{source_bucket}/{source_key}",
        "destination": f"s3://{BUCKET_NAME}/{DESTINATION_KEY}",
    }