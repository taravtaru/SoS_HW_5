import json
import os
from pathlib import Path

import httpx
import psycopg

SQL_DIR = Path(__file__).with_name("sql")


def sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text()


class ClickHouse:
    def __init__(self) -> None:
        self.client = httpx.Client(
            base_url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"),
            auth=(os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"]),
            timeout=httpx.Timeout(25, connect=3),
        )

    def query(self, query: str, **parameters) -> list[dict]:
        params = {f"param_{name}": str(value) for name, value in parameters.items()}
        params["output_format_json_quote_64bit_integers"] = "0"
        response = self._send(query + "\nFORMAT JSON", params=params)
        return response.json()["data"]

    def insert_snapshot(self, snapshot: dict) -> None:
        body = "INSERT INTO cinema.daily_snapshots FORMAT JSONEachRow\n" + json.dumps(snapshot)
        self._send(body)

    def _send(self, body: str, **kwargs) -> httpx.Response:
        response = self.client.post("/", content=body, **kwargs)
        if response.is_error:
            raise RuntimeError(f"ClickHouse HTTP {response.status_code}: {response.text.strip()}")
        return response


def connect_postgres() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=4,
        options="-c statement_timeout=15000",
    )
