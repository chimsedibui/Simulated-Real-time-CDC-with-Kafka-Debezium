"""Domain events, written in the same transaction as the change they describe."""

import json


def emit(cur, aggregate_type: str, aggregate_id, event_type: str, payload: dict):
    # business_now() reads the wall clock at call time (not the transaction start), so events of one
    # aggregate are ordered by when they happened, even if their transactions began out of order
    cur.execute(
        """INSERT INTO ecommerce.outbox_events (aggregatetype, aggregateid, type, payload, created_at)
           VALUES (%s, %s, %s, %s, ecommerce.business_now())""",
        (aggregate_type, str(aggregate_id), event_type, json.dumps(payload, default=str)),
    )
