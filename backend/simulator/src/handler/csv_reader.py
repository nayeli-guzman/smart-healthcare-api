import csv
import io
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

from .utils import normalize_value, sanitize_key


s3 = boto3.client("s3")


def build_header_mapping(fieldnames: List[str]) -> Dict[str, str]:
    """
    Creates clean column names and handles duplicate names.
    """
    mapping = {}
    seen = {}

    for raw_name in fieldnames or []:
        base = sanitize_key(raw_name)
        count = seen.get(base, 0)

        if count == 0:
            final_name = base
        else:
            final_name = f"{base}_{count + 1}"

        seen[base] = count + 1
        mapping[raw_name] = final_name

    return mapping


def read_csv_batch(
    bucket_name: str,
    dataset_key: str,
    start_row: int,
    batch_size: int
) -> Tuple[List[Tuple[int, Dict[str, Any]]], bool]:
    """
    Reads a batch of rows from the CSV file stored in S3.

    Returns:
    - rows: list of (row_number, normalized_row)
    - reached_eof: True when the end of the CSV was reached
    """
    try:
        response = s3.get_object(
            Bucket=bucket_name,
            Key=dataset_key
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")

        if error_code in {"NoSuchKey", "NoSuchBucket", "404"}:
            raise FileNotFoundError(
                f"Dataset not found at s3://{bucket_name}/{dataset_key}"
            ) from exc

        raise

    text_stream = io.TextIOWrapper(
        response["Body"],
        encoding="utf-8-sig",
        newline=""
    )

    reader = csv.DictReader(text_stream)
    header_mapping = build_header_mapping(reader.fieldnames or [])

    rows: List[Tuple[int, Dict[str, Any]]] = []
    reached_eof = True

    for row_number, row in enumerate(reader):
        if row_number < start_row:
            continue

        if len(rows) >= batch_size:
            reached_eof = False
            break

        normalized_row = {}

        for original_key, original_value in row.items():
            if original_key is None:
                continue

            clean_key = header_mapping.get(original_key, sanitize_key(original_key))
            normalized_row[clean_key] = normalize_value(original_value)

        rows.append((row_number, normalized_row))

    return rows, reached_eof