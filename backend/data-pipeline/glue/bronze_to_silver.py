import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date,
    to_timestamp,
    when
)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME",
        "BRONZE_PREFIX",
        "SILVER_PREFIX"
    ]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket_name = args["BUCKET_NAME"]
bronze_path = f"s3://{bucket_name}/{args['BRONZE_PREFIX']}"
silver_path = f"s3://{bucket_name}/{args['SILVER_PREFIX']}"

print(f"Reading Bronze data from: {bronze_path}")
print(f"Writing Silver data to: {silver_path}")

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(bronze_path)
)

print(f"Bronze record count: {df.count()}")

clean_df = df

# Convert timestamps
if "event_time" in clean_df.columns:
    clean_df = clean_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    )
else:
    clean_df = clean_df.withColumn(
        "event_timestamp",
        current_timestamp()
    )

clean_df = clean_df.withColumn(
    "event_date",
    to_date(col("event_timestamp"))
)

# Cast important clinical columns when present
double_columns = [
    "heart_rate",
    "respiratory_rate",
    "spo2_pct",
    "temperature_c",
    "systolic_bp",
    "diastolic_bp",
    "oxygen_flow",
    "wbc_count",
    "lactate",
    "creatinine",
    "crp_level",
    "hemoglobin",
    "sepsis_risk_score",
    "baseline_risk_score"
]

integer_columns = [
    "patient_id",
    "hour_from_admission",
    "mobility_score",
    "nurse_alert",
    "age",
    "comorbidity_index",
    "los_hours",
    "deterioration_event",
    "deterioration_within_12h_from_admission",
    "deterioration_hour",
    "deterioration_next_12h",
    "batch_id",
    "cycle",
    "csv_row_number"
]

for column_name in double_columns:
    if column_name in clean_df.columns:
        clean_df = clean_df.withColumn(
            column_name,
            col(column_name).cast("double")
        )

for column_name in integer_columns:
    if column_name in clean_df.columns:
        clean_df = clean_df.withColumn(
            column_name,
            col(column_name).cast("int")
        )

# Add an abnormal vitals flag
required_vital_columns = [
    "heart_rate",
    "respiratory_rate",
    "spo2_pct",
    "temperature_c",
    "systolic_bp"
]

if all(column_name in clean_df.columns for column_name in required_vital_columns):
    clean_df = clean_df.withColumn(
        "abnormal_vitals_flag",
        when(
            (col("heart_rate") > 120) |
            (col("respiratory_rate") > 24) |
            (col("spo2_pct") < 92) |
            (col("temperature_c") > 38.0) |
            (col("systolic_bp") < 90),
            lit(1)
        ).otherwise(lit(0))
    )
else:
    clean_df = clean_df.withColumn(
        "abnormal_vitals_flag",
        lit(None).cast("int")
    )

# Add a general risk category based on sepsis risk score when available
if "sepsis_risk_score" in clean_df.columns:
    clean_df = clean_df.withColumn(
        "sepsis_risk_category",
        when(col("sepsis_risk_score") >= 0.7, lit("HIGH"))
        .when(col("sepsis_risk_score") >= 0.4, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )
else:
    clean_df = clean_df.withColumn(
        "sepsis_risk_category",
        lit("UNKNOWN")
    )

# Add ETL metadata
clean_df = clean_df.withColumn(
    "silver_processed_at",
    current_timestamp()
)

# Filter out records without core simulation identifiers
if "sim_patient_id" in clean_df.columns:
    clean_df = clean_df.filter(col("sim_patient_id").isNotNull())

if "department" not in clean_df.columns:
    clean_df = clean_df.withColumn("department", lit("Unknown"))

if "event_date" not in clean_df.columns:
    clean_df = clean_df.withColumn("event_date", to_date(current_timestamp()))

print(f"Silver record count after cleaning: {clean_df.count()}")

(
    clean_df.write
    .mode("overwrite")
    .partitionBy("event_date", "department")
    .parquet(silver_path)
)

print("Bronze to Silver job completed successfully.")

job.commit()