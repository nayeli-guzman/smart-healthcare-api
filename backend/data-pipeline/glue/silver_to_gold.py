import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    current_timestamp,
    lit,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when
)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME",
        "SILVER_PREFIX",
        "GOLD_PREFIX"
    ]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket_name = args["BUCKET_NAME"]
silver_path = f"s3://{bucket_name}/{args['SILVER_PREFIX']}"
gold_path = f"s3://{bucket_name}/{args['GOLD_PREFIX']}"

print(f"Reading Silver data from: {silver_path}")
print(f"Writing Gold data to: {gold_path}")

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(silver_path)
)

print(f"Silver record count: {df.count()}")

# Guarantee required grouping columns exist
if "event_date" not in df.columns:
    if "event_timestamp" in df.columns:
        df = df.withColumn("event_date", to_date(col("event_timestamp")))
    else:
        df = df.withColumn("event_date", to_date(current_timestamp()))

if "department" not in df.columns:
    df = df.withColumn("department", lit("Unknown"))

if "admission_type" not in df.columns:
    df = df.withColumn("admission_type", lit("Unknown"))

if "sepsis_risk_category" not in df.columns:
    if "sepsis_risk_score" in df.columns:
        df = df.withColumn(
            "sepsis_risk_category",
            when(col("sepsis_risk_score") >= 0.7, lit("HIGH"))
            .when(col("sepsis_risk_score") >= 0.4, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
    else:
        df = df.withColumn("sepsis_risk_category", lit("UNKNOWN"))

# Ensure abnormal_vitals_flag exists
if "abnormal_vitals_flag" not in df.columns:
    vital_columns = [
        "heart_rate",
        "respiratory_rate",
        "spo2_pct",
        "temperature_c",
        "systolic_bp"
    ]

    if all(column_name in df.columns for column_name in vital_columns):
        df = df.withColumn(
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
        df = df.withColumn("abnormal_vitals_flag", lit(0))

# Ensure useful binary flags exist
if "deterioration_next_12h" in df.columns:
    df = df.withColumn(
        "deterioration_next_12h_flag",
        col("deterioration_next_12h").cast("int")
    )
else:
    df = df.withColumn("deterioration_next_12h_flag", lit(0))

if "nurse_alert" in df.columns:
    df = df.withColumn(
        "nurse_alert_flag",
        col("nurse_alert").cast("int")
    )
else:
    df = df.withColumn("nurse_alert_flag", lit(0))

if "sepsis_risk_score" in df.columns:
    df = df.withColumn(
        "high_sepsis_risk_flag",
        when(col("sepsis_risk_score") >= 0.7, lit(1)).otherwise(lit(0))
    )
else:
    df = df.withColumn("high_sepsis_risk_flag", lit(0))

# Aggregations for Gold analytics table
aggregations = [
    count("*").alias("total_records"),
    spark_sum("abnormal_vitals_flag").alias("abnormal_vitals_count"),
    spark_sum("deterioration_next_12h_flag").alias("deterioration_next_12h_count"),
    spark_sum("nurse_alert_flag").alias("nurse_alert_count"),
    spark_sum("high_sepsis_risk_flag").alias("high_sepsis_risk_count")
]

if "sim_patient_id" in df.columns:
    aggregations.append(
        approx_count_distinct("sim_patient_id").alias("estimated_unique_patients")
    )

if "sim_device_id" in df.columns:
    aggregations.append(
        approx_count_distinct("sim_device_id").alias("estimated_unique_devices")
    )

numeric_average_columns = [
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
    "baseline_risk_score",
    "age",
    "mobility_score",
    "comorbidity_index",
    "los_hours"
]

for column_name in numeric_average_columns:
    if column_name in df.columns:
        aggregations.append(
            spark_round(avg(col(column_name)), 4).alias(f"avg_{column_name}")
        )

gold_df = (
    df
    .groupBy(
        "event_date",
        "department",
        "admission_type",
        "sepsis_risk_category"
    )
    .agg(*aggregations)
)

gold_df = (
    gold_df
    .withColumn(
        "abnormal_vitals_rate",
        spark_round(col("abnormal_vitals_count") / col("total_records"), 4)
    )
    .withColumn(
        "deterioration_next_12h_rate",
        spark_round(col("deterioration_next_12h_count") / col("total_records"), 4)
    )
    .withColumn(
        "nurse_alert_rate",
        spark_round(col("nurse_alert_count") / col("total_records"), 4)
    )
    .withColumn(
        "high_sepsis_risk_rate",
        spark_round(col("high_sepsis_risk_count") / col("total_records"), 4)
    )
    .withColumn(
        "gold_processed_at",
        current_timestamp()
    )
)

print(f"Gold record count: {gold_df.count()}")

(
    gold_df.write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(gold_path)
)

print("Silver to Gold job completed successfully.")

job.commit()