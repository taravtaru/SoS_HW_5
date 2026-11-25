import json
import os
from contextlib import closing
from datetime import UTC, date

import boto3
from botocore.config import Config

from app.storage import connect_postgres

BUCKET = "movie-analytics"


class MissingMetrics(Exception):
    pass


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://s3:8333"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(
            connect_timeout=3, read_timeout=8,
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )


def pending_exports(before: date) -> list[date]:
    with connect_postgres() as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT m.metric_date
            FROM daily_metrics AS m
            LEFT JOIN daily_exports AS e USING (metric_date)
            WHERE m.metric_date < %s AND e.metric_date IS NULL
            ORDER BY m.metric_date
            """,
            (before,),
        )
        return [row[0] for row in rows]


def export_day(day: date) -> str:
    with connect_postgres() as connection:
        rows = connection.execute(
            """
            SELECT metric_name, dimension, value, computed_at
            FROM daily_metrics
            WHERE metric_date = %s
            ORDER BY metric_name, dimension
            """,
            (day,),
        ).fetchall()
        if not rows:
            raise MissingMetrics(f"No computed metrics for {day}")
        connection.execute("DELETE FROM daily_exports WHERE metric_date = %s", (day,))

    payload = {
        "date": day.isoformat(),
        "metrics": [
            {
                "name": name,
                "dimension": dimension or None,
                "value": value,
                "computed_at": computed_at.astimezone(UTC).isoformat(),
            }
            for name, dimension, value, computed_at in rows
        ],
    }
    key = f"daily/{day}/aggregates.json"
    with closing(s3_client()) as client:
        client.put_object(
            Bucket=BUCKET, Key=key, ContentType="application/json",
            Body=json.dumps(payload, ensure_ascii=False, allow_nan=False).encode(),
        )

    with connect_postgres() as connection:
        connection.execute(
            """
            INSERT INTO daily_exports (metric_date) VALUES (%s)
            ON CONFLICT (metric_date) DO UPDATE SET exported_at = now()
            """,
            (day,),
        )
    return f"s3://{BUCKET}/{key}"
