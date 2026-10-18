"""Read-only queries against the ClickHouse bronze/silver/gold layers."""

import datetime as dt
import threading
from decimal import Decimal

import clickhouse_connect

class Warehouse:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.cfg = {"host": host, "port": port, "username": user, "password": password}
        self.local = threading.local()

    def _client(self):
        # clickhouse-connect clients are not thread-safe; FastAPI runs sync routes in a pool
        if getattr(self.local, "client", None) is None:
            self.local.client = clickhouse_connect.get_client(**self.cfg)
        return self.local.client

    def rows(self, sql: str) -> list:
        try:
            result = self._client().query(sql)
        except Exception:
            self.local.client = None
            raise
        return [
            {k: _jsonable(v) for k, v in zip(result.column_names, row)}
            for row in result.result_rows
        ]

    def ping(self) -> bool:
        try:
            return self._client().ping()
        except Exception:
            self.local.client = None
            return False

def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if hasattr(v, "hex") and not isinstance(v, (bytes, str, int, float)):  # UUID
        return str(v)
    return v
