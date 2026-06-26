import os
import boto3
import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


BUCKET_NAME = os.environ["BUCKET_NAME"]
INPUT_KEY = os.environ["INPUT_KEY"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]

LOCAL_INPUT = "forecast_training_data.csv"
LOCAL_FORECAST = "forecast_results.csv"
LOCAL_METRICS = "model_metrics.csv"
LOCAL_PLOT = "forecast_plot.png"

s3 = boto3.client("s3")


def download_input():
    print(f"Downloading s3://{BUCKET_NAME}/{INPUT_KEY}")
    s3.download_file(BUCKET_NAME, INPUT_KEY, LOCAL_INPUT)


def upload_output(local_file, s3_key):
    print(f"Uploading {local_file} to s3://{BUCKET_NAME}/{s3_key}")
    s3.upload_file(local_file, BUCKET_NAME, s3_key)


def main():
    download_input()

    df = pd.read_csv(LOCAL_INPUT)

    print("Columns found:")
    print(df.columns.tolist())
    print(f"Rows found: {len(df)}")

    target = "deterioration_cases"

    numeric_features = [
        "time_index",
        "total_records",
        "abnormal_vitals_cases",
        "nurse_alert_cases",
        "high_sepsis_risk_cases",
        "avg_sepsis_risk_score",
        "avg_heart_rate",
        "avg_respiratory_rate",
        "avg_spo2_pct",
        "avg_temperature_c",
    ]

    categorical_features = [
        "department",
        "admission_type",
    ]

    required_columns = numeric_features + categorical_features + [target]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in CSV: {missing_columns}")

    df = df[required_columns].copy()

    for col in numeric_features + [target]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in categorical_features:
        df[col] = df[col].fillna("Unknown").astype(str)

    df = df.dropna(subset=[target])

    for col in numeric_features:
        df[col] = df[col].fillna(df[col].median())

    if len(df) < 10:
        raise ValueError("Not enough rows for training. Generate/export more data first.")

    X = df[numeric_features + categorical_features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.25,
        random_state=42,
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("numeric", "passthrough", numeric_features),
        ]
    )

    model = RandomForestRegressor(
        n_estimators=150,
        max_depth=8,
        random_state=42,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_pred = np.maximum(y_pred, 0)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    r2 = r2_score(y_test, y_pred)

    results = X_test.copy()
    results["actual_deterioration_cases"] = y_test.values
    results["forecasted_deterioration_cases"] = y_pred.round(2)

    results = results.sort_values(
        ["time_index", "department", "admission_type"]
    )

    results.to_csv(LOCAL_FORECAST, index=False)

    metrics_df = pd.DataFrame(
        [
            {
                "metric": "Rows used",
                "value": len(df),
                "description": "Total rows used by the SageMaker forecasting model",
            },
            {
                "metric": "Training rows",
                "value": len(X_train),
                "description": "Rows used for training",
            },
            {
                "metric": "Testing rows",
                "value": len(X_test),
                "description": "Rows used for testing",
            },
            {
                "metric": "MAE",
                "value": round(mae, 4),
                "description": "Mean absolute error",
            },
            {
                "metric": "RMSE",
                "value": round(rmse, 4),
                "description": "Root mean squared error",
            },
            {
                "metric": "R2 score",
                "value": round(r2, 4),
                "description": "Model explanatory power",
            },
        ]
    )

    metrics_df.to_csv(LOCAL_METRICS, index=False)

    plot_df = (
        results
        .groupby("time_index")[["actual_deterioration_cases", "forecasted_deterioration_cases"]]
        .sum()
        .reset_index()
        .sort_values("time_index")
    )

    plt.figure(figsize=(10, 5))
    plt.plot(
        plot_df["time_index"],
        plot_df["actual_deterioration_cases"],
        marker="o",
        label="Actual"
    )
    plt.plot(
        plot_df["time_index"],
        plot_df["forecasted_deterioration_cases"],
        marker="o",
        label="Forecast"
    )
    plt.xlabel("Hour from admission")
    plt.ylabel("Deterioration cases")
    plt.title("Actual vs Forecasted Clinical Deterioration Volume")
    plt.legend()
    plt.tight_layout()
    plt.savefig(LOCAL_PLOT)

    upload_output(LOCAL_FORECAST, OUTPUT_PREFIX + LOCAL_FORECAST)
    upload_output(LOCAL_METRICS, OUTPUT_PREFIX + LOCAL_METRICS)
    upload_output(LOCAL_PLOT, OUTPUT_PREFIX + LOCAL_PLOT)

    print("")
    print("Forecasting complete.")
    print(f"Uploaded: s3://{BUCKET_NAME}/{OUTPUT_PREFIX}{LOCAL_FORECAST}")
    print(f"Uploaded: s3://{BUCKET_NAME}/{OUTPUT_PREFIX}{LOCAL_METRICS}")
    print(f"Uploaded: s3://{BUCKET_NAME}/{OUTPUT_PREFIX}{LOCAL_PLOT}")


if __name__ == "__main__":
    main()