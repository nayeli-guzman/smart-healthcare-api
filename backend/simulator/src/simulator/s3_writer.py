import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3

from .config import BUCKET_NAME


s3 = boto3.client("s3")


def write_events_to_s3(events: List[Dict[str, Any]], batch_id: int) -> str:
    """
    Writes one Lambda batch as newline-delimited JSON into the Bronze zone.
    This replaces Firehose temporarily while Firehose is blocked in the AWS account.
    """
    now = datetime.now(timezone.utc)

    key = (
        "bronze/clinical-deterioration/"
        f"year={now.year:04d}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"batch-{batch_id:08d}.json"
    )

    body = "\n".join(
        json.dumps(event, default=str)
        for event in events
    ) + "\n"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    return f"s3://{BUCKET_NAME}/{key}"