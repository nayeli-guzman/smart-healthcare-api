import os

BUCKET_NAME = os.environ["BUCKET_NAME"]
DATASET_KEY = os.environ["DATASET_KEY"]
STATE_TABLE_NAME = os.environ["STATE_TABLE_NAME"]
FIREHOSE_STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]

DATASET_NAME = "hospital_clinical_deterioration"

DEFAULT_BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "25"))
RESET_ON_EOF = os.environ.get("RESET_ON_EOF", "true").lower() == "true"

MAX_FIREHOSE_BATCH_SIZE = 500

DEPARTMENTS = [
    "Emergency",
    "ICU",
    "Cardiology",
    "Internal Medicine",
    "Surgery",
    "General Ward"
]