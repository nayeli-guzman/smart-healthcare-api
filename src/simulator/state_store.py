from typing import Any, Dict

import boto3

from .config import DATASET_NAME, STATE_TABLE_NAME
from .utils import utc_now_iso


dynamodb = boto3.resource("dynamodb")
state_table = dynamodb.Table(STATE_TABLE_NAME)


def get_state() -> Dict[str, Any]:
    response = state_table.get_item(
        Key={
            "dataset_name": DATASET_NAME
        }
    )

    return response.get("Item", {})


def update_state(
    next_row: int,
    batch_id: int,
    cycle: int,
    sent_count: int,
    status: str
) -> None:
    state_table.update_item(
        Key={
            "dataset_name": DATASET_NAME
        },
        UpdateExpression=(
            "SET next_row = :next_row, "
            "batch_id = :batch_id, "
            "#cycle = :cycle, "
            "last_run_at = :last_run_at, "
            "last_status = :status, "
            "last_sent_count = :sent_count, "
            "total_sent = if_not_exists(total_sent, :zero) + :sent_count"
        ),
        ExpressionAttributeNames={
            "#cycle": "cycle"
        },
        ExpressionAttributeValues={
            ":next_row": next_row,
            ":batch_id": batch_id,
            ":cycle": cycle,
            ":last_run_at": utc_now_iso(),
            ":status": status,
            ":sent_count": sent_count,
            ":zero": 0
        }
    )