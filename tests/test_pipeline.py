import json
import unittest
from contextlib import closing
from datetime import UTC, date, datetime, time, timedelta, timezone
from time import monotonic, sleep
from uuid import uuid4

import httpx
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from app.export import BUCKET, s3_client
from app.storage import ClickHouse, connect_postgres

PRODUCER = "http://producer:8000"
AGGREGATION = "http://aggregation:8001"
REGISTRY = "http://schema-registry:8081"
GRAFANA = "http://grafana:3000"
BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"


def wait_for(check, description, timeout=45):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        result = check()
        if result:
            return result
        sleep(0.5)
    raise AssertionError(f"Timed out: {description}")


def event(day: date, user: str, kind: str, progress: int | None, movie="orbit") -> dict:
    timestamp = datetime.combine(day, time(12, 34, 56, 123000), UTC)
    if kind == "VIEW_STARTED":
        timestamp = timestamp.astimezone(timezone(timedelta(hours=3)))
    elif kind == "VIEW_FINISHED":
        timestamp += timedelta(seconds=progress)
    return {
        "event_id": str(uuid4()),
        "user_id": user,
        "movie_id": movie,
        "event_type": kind,
        "timestamp": timestamp.isoformat(),
        "device_type": "TV",
        "session_id": f"session-{user}-{movie}-{day}",
        "progress_seconds": progress,
    }


def postgres_metrics(day: date) -> dict:
    with connect_postgres() as connection:
        rows = connection.execute(
            """
            SELECT metric_name, dimension, value, computed_at
            FROM daily_metrics WHERE metric_date = %s
            """,
            (day,),
        )
        return {(name, dimension): (value, computed_at) for name, dimension, value, computed_at in rows}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.http = httpx.Client(timeout=25)
        self.addCleanup(self.http.close)
        self.clickhouse = ClickHouse()
        self.addCleanup(self.clickhouse.client.close)

    def publish(self, record):
        response = self.http.post(f"{PRODUCER}/events", json=record)
        self.assertEqual(response.status_code, 201, response.text)
        self.assertEqual(response.json(), {"event_id": record["event_id"]})

    def read_event(self, event_id):
        return self.clickhouse.query(
            """
            SELECT event_id, user_id, movie_id, event_type,
                   toUnixTimestamp64Milli(timestamp) AS timestamp, device_type,
                   session_id, progress_seconds
            FROM cinema.raw_events FINAL
            WHERE event_id = {id:UUID}
            """,
            id=event_id,
        )

    def test_pipeline(self):
        admin = AdminClient({"bootstrap.servers": BROKERS})
        metadata = admin.list_topics(topic="movie-events", timeout=10).topics["movie-events"]
        self.assertIsNone(metadata.error)
        self.assertEqual(len(metadata.partitions), 3)
        self.assertTrue(all(len(p.replicas) == 2 for p in metadata.partitions.values()))
        topic = ConfigResource(ResourceType.TOPIC, "movie-events")
        config = admin.describe_configs([topic], request_timeout=10)[topic].result()
        self.assertEqual(config["min.insync.replicas"].value, "1")
        schema = self.http.get(f"{REGISTRY}/subjects/movie-events-value/versions/1").raise_for_status().json()
        self.assertEqual(schema["version"], 1)
        self.assertEqual(json.loads(schema["schema"])["name"], "MovieEvent")

        day = datetime.now(UTC).date() - timedelta(days=10)
        main = event(day, "alice", "VIEW_STARTED", 0)
        invalid = [
            {**main, "progress_seconds": None},
            {**main, "event_type": "WATCHED"},
            {**main, "timestamp": "2026-01-04T12:00:00"},
        ]
        for record in invalid:
            response = self.http.post(f"{PRODUCER}/events", json=record)
            self.assertEqual(response.status_code, 422, response.text)

        fixtures = [
            event(day - timedelta(days=1), "alice", "SEARCHED", None),
            event(day - timedelta(days=1), "returning", "VIEW_STARTED", 0),
            main,
            event(day, "alice", "VIEW_FINISHED", 120),
            event(day, "bob", "VIEW_STARTED", 0),
            event(day, "alice", "VIEW_STARTED", 0, "signal"),
            event(day, "alice", "VIEW_FINISHED", 180, "signal"),
            event(day, "returning", "LIKED", None),
            event(day + timedelta(days=1), "alice", "SEARCHED", None),
            event(day + timedelta(days=1), "returning", "VIEW_STARTED", 0),
            event(day + timedelta(days=7), "bob", "VIEW_STARTED", 0),
        ]
        for record in fixtures:
            self.publish(record)
        self.publish(main)

        wait_for(
            lambda: len(self.clickhouse.query("SELECT event_id FROM cinema.raw_events FINAL")) == len(fixtures),
            "all unique events in ClickHouse",
        )
        for record in fixtures:
            expected = {
                **record, "timestamp": round(datetime.fromisoformat(record["timestamp"]).timestamp() * 1000),
            }
            self.assertEqual(self.read_event(record["event_id"]), [expected])

        response = self.http.post(f"{AGGREGATION}/export/{day}")
        self.assertEqual(response.status_code, 404, response.text)
        for _ in range(2):
            response = self.http.post(f"{AGGREGATION}/aggregate/{day}").raise_for_status()
            self.assertEqual(response.json(), {"date": day.isoformat(), "processed_events": 6})

        snapshots = self.clickhouse.query(
            "SELECT * FROM cinema.daily_snapshots FINAL WHERE metric_date = {day:Date}", day=day,
        )
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot["dau"], 3)
        self.assertEqual(snapshot["average_watch_seconds"], 150)
        self.assertAlmostEqual(snapshot["view_conversion"], 2 / 3)
        self.assertEqual(snapshot["top_movies"], [["orbit", 2], ["signal", 1]])
        self.assertEqual(snapshot["cohort_size"], 2)
        retention = [1, 0.5, 0, 0, 0, 0, 0, 0.5]
        self.assertEqual(snapshot["retention"], retention)

        expected_values = {
            ("dau", ""): 3, ("average_watch_seconds", ""): 150, ("view_conversion", ""): 2 / 3,
            ("cohort_size", ""): 2, ("movie_views", "orbit"): 2, ("movie_views", "signal"): 1,
            **{(f"retention_d{i}", ""): value for i, value in enumerate(retention)},
        }
        stored = postgres_metrics(day)
        self.assertEqual({key: value for key, (value, _) in stored.items()}, expected_values)
        self.assertEqual(len({computed_at for _, computed_at in stored.values()}), 1)

        key = f"daily/{day}/aggregates.json"
        for _ in range(2):
            response = self.http.post(f"{AGGREGATION}/export/{day}").raise_for_status()
            self.assertEqual(response.json(), {"date": day.isoformat(), "uri": f"s3://{BUCKET}/{key}"})
        with closing(s3_client()) as client:
            objects = client.list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{day}/")
            self.assertEqual([item["Key"] for item in objects["Contents"]], [key])
            body = client.get_object(Bucket=BUCKET, Key=key)["Body"]
            with closing(body):
                exported = json.loads(body.read())

        self.assertEqual(exported["date"], day.isoformat())
        self.assertEqual({
            (metric["name"], metric["dimension"] or ""): (
                metric["value"], datetime.fromisoformat(metric["computed_at"]),
            ) for metric in exported["metrics"]
        }, stored)

        dashboard = self.http.get(f"{GRAFANA}/api/dashboards/uid/cinema-analytics").raise_for_status().json()
        panels = dashboard["dashboard"]["panels"]
        self.assertEqual(len(panels), 3)
        start = datetime.combine(day, time.min, UTC)
        time_range = {"from": str(int(start.timestamp() * 1000)),
                      "to": str(int((start + timedelta(hours=23)).timestamp() * 1000))}
        for panel, expected in zip(panels, (retention, [3], [2 / 3]), strict=True):
            response = self.http.post(f"{GRAFANA}/api/ds/query", json={
                **time_range, "queries": panel["targets"],
            })
            self.assertEqual(response.status_code, 200, response.text)
            result = response.json()["results"]["A"]
            self.assertIn("frames", result, result)
            fields = result["frames"][0]["data"]["values"]
            self.assertEqual([field[0] for field in fields[1:]], expected)

        wait_for(
            lambda: all(len(p.isrs) == 2 for p in admin.list_topics(
                topic="movie-events", timeout=5,
            ).topics["movie-events"].partitions.values()),
            "all replicas in sync before failover",
        )


class FailoverTests(unittest.TestCase):
    def test_ingestion_with_one_broker_stopped(self):
        record = event(datetime.now(UTC).date(), "failover", "VIEW_STARTED", 0)
        with httpx.Client(timeout=25) as client:
            admin = AdminClient({"bootstrap.servers": BROKERS})
            wait_for(lambda: 1 not in admin.list_topics(timeout=5).brokers, "kafka-1 removed from metadata")
            response = client.post(f"{PRODUCER}/events", json=record)
            response.raise_for_status()
            self.assertEqual(response.json()["event_id"], record["event_id"])

        clickhouse = ClickHouse()
        self.addCleanup(clickhouse.client.close)
        wait_for(
            lambda: clickhouse.query(
                "SELECT event_id FROM cinema.raw_events FINAL WHERE event_id = {id:UUID}",
                id=record["event_id"],
            ),
            "event reaches ClickHouse with kafka-1 stopped",
        )


if __name__ == "__main__":
    unittest.main()
