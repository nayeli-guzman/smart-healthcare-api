from datetime import datetime, timezone

from .config import (
    BUCKET_NAME,
    DATASET_KEY,
    DATASET_NAME,
    DEFAULT_BATCH_SIZE,
    RESET_ON_EOF
)
from .csv_reader import read_csv_batch
from .event_builder import build_simulated_event
from .s3_writer import write_events_to_s3
from .state_store import get_state, update_state


def lambda_handler(event, context):
    """
    Lambda entry point.

    Every execution:
    1. Reads current CSV position from DynamoDB.
    2. Reads the next N rows from the Kaggle CSV in S3.
    3. Converts each row into an IoT-style clinical event.
    4. Writes the batch into S3 Bronze as newline-delimited JSON.
    5. Updates DynamoDB with the next row position.
    """
    batch_size = DEFAULT_BATCH_SIZE

    if isinstance(event, dict) and event.get("batch_size"):
        batch_size = int(event["batch_size"])

    batch_size = max(1, batch_size)

    state = get_state()

    start_row = int(state.get("next_row", 0))
    previous_batch_id = int(state.get("batch_id", 0))
    cycle = int(state.get("cycle", 0))

    batch_id = previous_batch_id + 1

    print(
        f"Starting IoT CSV replay. "
        f"dataset={DATASET_NAME}, start_row={start_row}, "
        f"batch_size={batch_size}, batch_id={batch_id}, cycle={cycle}"
    )

    try:
        rows, reached_eof = read_csv_batch(
            bucket_name=BUCKET_NAME,
            dataset_key=DATASET_KEY,
            start_row=start_row,
            batch_size=batch_size
        )
    except FileNotFoundError as exc:
        print(str(exc))
        update_state(
            next_row=start_row,
            batch_id=batch_id,
            cycle=cycle,
            sent_count=0,
            status="dataset_missing"
        )
        return {
            "statusCode": 200,
            "message": "Dataset missing. No records sent.",
            "expected_s3_path": f"s3://{BUCKET_NAME}/{DATASET_KEY}"
        }

    if not rows and reached_eof and RESET_ON_EOF:
        print("Reached end of CSV. Restarting from row 0 because RESET_ON_EOF=true.")
        cycle += 1
        start_row = 0

        rows, reached_eof = read_csv_batch(
            bucket_name=BUCKET_NAME,
            dataset_key=DATASET_KEY,
            start_row=start_row,
            batch_size=batch_size
        )

    if not rows:
        print("No rows found. Nothing sent.")
        update_state(
            next_row=start_row,
            batch_id=batch_id,
            cycle=cycle,
            sent_count=0,
            status="empty_or_completed"
        )
        return {
            "statusCode": 200,
            "message": "No rows found. Nothing sent."
        }

    base_time = datetime.now(timezone.utc)

    simulated_events = [
        build_simulated_event(
            row_number=row_number,
            row_data=row_data,
            batch_id=batch_id,
            cycle=cycle,
            position_in_batch=index,
            base_time=base_time
        )
        for index, (row_number, row_data) in enumerate(rows)
    ]

    bronze_file_path = write_events_to_s3(
        events=simulated_events,
        batch_id=batch_id
    )

    sent_count = len(simulated_events)

    if reached_eof and RESET_ON_EOF:
        next_row = 0
        next_cycle = cycle + 1
        status = "sent_and_reset_to_start"
    else:
        next_row = start_row + sent_count
        next_cycle = cycle
        status = "sent"

    update_state(
        next_row=next_row,
        batch_id=batch_id,
        cycle=next_cycle,
        sent_count=sent_count,
        status=status
    )

    print(
        f"IoT replay completed. sent_count={sent_count}, "
        f"next_row={next_row}, status={status}, bronze_file={bronze_file_path}"
    )

    return {
        "statusCode": 200,
        "dataset": DATASET_NAME,
        "batch_id": batch_id,
        "cycle": next_cycle,
        "sent_count": sent_count,
        "next_row": next_row,
        "status": status,
        "bronze_file": bronze_file_path,
        "bronze_destination": f"s3://{BUCKET_NAME}/bronze/clinical-deterioration/"
    }