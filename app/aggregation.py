import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from threading import Event, Lock, Thread
from time import monotonic, sleep

import psycopg
from fastapi import FastAPI, HTTPException, Request

from app.export import MissingMetrics, export_day, pending_exports
from app.storage import ClickHouse, connect_postgres, sql

logger = logging.getLogger("uvicorn.error")
MAX_RETENTION_DAY = 7
DAILY_QUERY = sql("daily.sql")
TOP_MOVIES_QUERY = sql("top_movies.sql")
RETENTION_QUERY = sql("retention.sql")

UPSERT = """
    INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (metric_date, metric_name, dimension) DO UPDATE
    SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
"""


def utc_today() -> date:
    return datetime.now(UTC).date()


def save_metrics(day: date, computed_at: datetime, values: dict) -> None:
    rows = [(day, name, dimension, value, computed_at) for (name, dimension), value in values.items()]
    for attempt in range(3):
        try:
            with connect_postgres() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT metric_name, dimension, value FROM daily_metrics WHERE metric_date = %s",
                    (day,),
                )
                previous = {(name, dimension): value for name, dimension, value in cursor}
                cursor.executemany(UPSERT, rows)
                cursor.execute(
                    "DELETE FROM daily_metrics WHERE metric_date = %s AND computed_at <> %s",
                    (day, computed_at),
                )
                if previous != values:
                    cursor.execute("DELETE FROM daily_exports WHERE metric_date = %s", (day,))
            return
        except psycopg.OperationalError as error:
            logger.warning("postgres_write_failed date=%s attempt=%s error=%s", day, attempt + 1, error)
            if attempt == 2:
                raise
            sleep(2**attempt)


class Analytics:
    def __init__(self) -> None:
        self.clickhouse = ClickHouse()
        self.lock = Lock()

    def recalculate(self, day: date) -> int:
        with self.lock:
            start = monotonic()
            logger.info("aggregation_started date=%s", day)
            try:
                return self._recalculate(day, start)
            except Exception:
                logger.exception("aggregation_failed date=%s elapsed_seconds=%.3f", day, monotonic() - start)
                raise

    def _recalculate(self, day: date, start: float) -> int:
        stats = self.clickhouse.query(DAILY_QUERY, day=day)[0]
        movies = self.clickhouse.query(TOP_MOVIES_QUERY, day=day)
        returns = {
            row["day_offset"]: row["users"]
            for row in self.clickhouse.query(RETENTION_QUERY, day=day)
        }
        age = (utc_today() - day).days
        cohort_size = returns.get(0, 0)
        retention = [
            returns.get(offset, 0) / cohort_size if cohort_size and offset <= age else None
            for offset in range(MAX_RETENTION_DAY + 1)
        ]
        computed_at = datetime.now(UTC)

        self.clickhouse.insert_snapshot({
            **stats,
            "metric_date": day.isoformat(),
            "top_movies": [[movie["movie_id"], movie["views"]] for movie in movies],
            "cohort_size": cohort_size,
            "retention": retention,
            "computed_at": computed_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
        })

        values = {
            ("dau", ""): stats["dau"],
            ("average_watch_seconds", ""): stats["average_watch_seconds"],
            ("view_conversion", ""): stats["view_conversion"],
            ("cohort_size", ""): cohort_size,
        }
        values.update({(f"retention_d{offset}", ""): rate for offset, rate in enumerate(retention)})
        values.update({("movie_views", movie["movie_id"]): movie["views"] for movie in movies})
        save_metrics(day, computed_at, values)

        processed = stats["processed_events"]
        logger.info(
            "aggregation_finished date=%s processed_events=%s elapsed_seconds=%.3f",
            day, processed, monotonic() - start,
        )
        return processed

    def export(self, day: date) -> str:
        with self.lock:
            try:
                uri = export_day(day)
            except MissingMetrics:
                raise
            except Exception:
                logger.exception("export_failed date=%s", day)
                raise
            logger.info("export_finished date=%s uri=%s", day, uri)
            return uri

    def schedule(self, interval: float, stop: Event) -> None:
        while not stop.is_set():
            today = utc_today()
            for days_ago in range(MAX_RETENTION_DAY + 1, -1, -1):
                if stop.is_set():
                    return
                try:
                    self.recalculate(today - timedelta(days=days_ago))
                except Exception:
                    # The date is retried on the next scheduled cycle.
                    continue

            try:
                days = pending_exports(today)
            except psycopg.Error:
                logger.exception("export_queue_read_failed")
                days = []
            for day in days:
                if stop.is_set():
                    return
                try:
                    self.export(day)
                except MissingMetrics:
                    logger.warning("export_metrics_missing date=%s", day)
                except Exception:
                    # A failed upload remains pending in PostgreSQL.
                    continue
            stop.wait(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with connect_postgres() as connection:
        connection.execute(sql("postgres.sql"))
    analytics = app.state.analytics = Analytics()
    interval = float(os.getenv("AGGREGATION_INTERVAL_SECONDS", "60"))
    stop = Event()
    worker = None
    if interval > 0:
        worker = Thread(target=analytics.schedule, args=(interval, stop), daemon=True)
        worker.start()
    try:
        yield
    finally:
        stop.set()
        if worker:
            worker.join(timeout=2)
        if worker is None or not worker.is_alive():
            analytics.clickhouse.client.close()


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


def check_date(day: date) -> None:
    if not date(1970, 1, 1) <= day <= utc_today():
        raise HTTPException(422, "Date must be between 1970-01-01 and today (UTC)")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/aggregate/{day}")
def recalculate(day: date, request: Request):
    check_date(day)
    try:
        processed = request.app.state.analytics.recalculate(day)
    except Exception as error:
        raise HTTPException(503, "Aggregation failed") from error
    return {"date": day, "processed_events": processed}


@app.post("/export/{day}")
def export(day: date, request: Request):
    check_date(day)
    try:
        uri = request.app.state.analytics.export(day)
    except MissingMetrics as error:
        raise HTTPException(404, str(error)) from error
    except Exception as error:
        raise HTTPException(503, "Export failed") from error
    return {"date": day, "uri": uri}
