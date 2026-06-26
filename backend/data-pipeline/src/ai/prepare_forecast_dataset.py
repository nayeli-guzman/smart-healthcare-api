import os
import time
import urllib.parse

import boto3


athena = boto3.client("athena")
s3 = boto3.client("s3")


BUCKET_NAME = os.environ["BUCKET_NAME"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
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
      CAST(hour_from_admission AS INTEGER) AS time_index,
      department,
      admission_type,
      COUNT(*) AS total_records,
      SUM(CAST(deterioration_next_12h AS INTEGER)) AS deterioration_cases,
      SUM(CAST(abnormal_vitals_flag AS INTEGER)) AS abnormal_vitals_cases,
      SUM(CAST(nurse_alert AS INTEGER)) AS nurse_alert_cases,
      SUM(CASE WHEN sepsis_risk_category = 'HIGH' THEN 1 ELSE 0 END) AS high_sepsis_risk_cases,
      ROUND(AVG(sepsis_risk_score), 4) AS avg_sepsis_risk_score,
      ROUND(AVG(heart_rate), 2) AS avg_heart_rate,
      ROUND(AVG(respiratory_rate), 2) AS avg_respiratory_rate,
      ROUND(AVG(spo2_pct), 2) AS avg_spo2_pct,
      ROUND(AVG(temperature_c), 2) AS avg_temperature_c
    FROM {DATABASE_NAME}.silver_clinical_deterioration
    WHERE hour_from_admission IS NOT NULL
    GROUP BY
      CAST(hour_from_admission AS INTEGER),
      department,
      admission_type
    ORDER BY
      time_index,
      department,
      admission_type
    """

    print("Starting Athena query for forecasting dataset")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": DATABASE_NAME
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
        raise Exception(f"Athena query failed. State={final_state}. Reason={reason}")

    source_bucket, source_key = parse_s3_uri(output_location)

    print(f"Athena output: s3://{source_bucket}/{source_key}")
    print(f"Copying to fixed ML input: s3://{BUCKET_NAME}/{DESTINATION_KEY}")

    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={
            "Bucket": source_bucket,
            "Key": source_key,
        },
        Key=DESTINATION_KEY,
    )

    return {
        "status": "prepared",
        "query_execution_id": query_execution_id,
        "source": f"s3://{source_bucket}/{source_key}",
        "destination": f"s3://{BUCKET_NAME}/{DESTINATION_KEY}",
    }