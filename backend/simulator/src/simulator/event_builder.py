import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict

from .config import DATASET_NAME, DEPARTMENTS
from .utils import utc_now_iso


def build_simulated_event(
    row_number: int,
    row_data: Dict[str, Any],
    batch_id: int,
    cycle: int,
    position_in_batch: int,
    base_time: datetime
) -> Dict[str, Any]:
    """
    Converts one CSV row into one IoT-style clinical event.
    """
    event_time = base_time + timedelta(seconds=position_in_batch)

    patient_hash = hashlib.sha256(
        f"{DATASET_NAME}:{row_number}".encode("utf-8")
    ).hexdigest()[:12]

    department = DEPARTMENTS[row_number % len(DEPARTMENTS)]

    event = {
        "source_dataset": "Hospital Clinical Deterioration Dataset - Kaggle",
        "simulation_type": "iot_csv_replay",
        "batch_id": batch_id,
        "cycle": cycle,
        "csv_row_number": row_number,

        "sim_patient_id": f"P-{patient_hash}",
        "sim_device_id": f"clinical-device-{(row_number % 50) + 1:03d}",
        "sim_gateway_id": f"hospital-gateway-{(row_number % 5) + 1:02d}",
        "department": department,

        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "ingestion_time": utc_now_iso()
    }

    # Keep original CSV data at the top level.
    # This makes the future Glue Crawler and Athena tables easier.
    event.update(row_data)

    return event