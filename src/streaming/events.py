"""Deterministic event construction from the static image manifest."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


EVENT_NAMESPACE = uuid.UUID("18ddc104-358d-4a9b-bae8-32bd0e1e211d")


def make_image_event(filename: str, validation_status: str, event_run_id: str = "static-manifest") -> dict:
    event_key = f"{event_run_id}:image_validated:{filename}"
    return {
        "event_id": str(uuid.uuid5(EVENT_NAMESPACE, event_key)),
        "event_run_id": event_run_id,
        "filename": filename,
        "event_type": "image_validated",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "validation_status": validation_status,
        "schema_version": 1,
    }
