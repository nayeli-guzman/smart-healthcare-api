import json
import time
from typing import Any, Dict, List

import boto3

from .config import FIREHOSE_STREAM_NAME, MAX_FIREHOSE_BATCH_SIZE


firehose = boto3.client("firehose")


def put_records_to_firehose(events: List[Dict[str, Any]]) -> int:
    """
    Sends events to Firehose in batches.
    """
    total_sent = 0

    for start in range(0, len(events), MAX_FIREHOSE_BATCH_SIZE):
        chunk_events = events[start:start + MAX_FIREHOSE_BATCH_SIZE]

        records = [
            {
                "Data": (json.dumps(event, default=str) + "\n").encode("utf-8")
            }
            for event in chunk_events
        ]

        pending_records = records

        for attempt in range(1, 4):
            response = firehose.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM_NAME,
                Records=pending_records
            )

            failed_count = response.get("FailedPutCount", 0)

            if failed_count == 0:
                total_sent += len(pending_records)
                pending_records = []
                break

            request_responses = response.get("RequestResponses", [])
            failed_records = []

            for original_record, result in zip(pending_records, request_responses):
                if "ErrorCode" in result:
                    failed_records.append(original_record)

            print(
                f"Firehose attempt {attempt} failed for {failed_count} records. "
                f"Retrying {len(failed_records)} records."
            )

            pending_records = failed_records
            time.sleep(1)

        if pending_records:
            raise RuntimeError(
                f"Failed to send {len(pending_records)} records to Firehose after retries."
            )

    return total_sent