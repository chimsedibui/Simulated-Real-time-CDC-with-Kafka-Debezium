"""CDC Control Center — drives the traffic simulator, tails the CDC stream, reads the ClickHouse warehouse.

It never writes to PostgreSQL: generator controls are proxied to the simulator,
which changes data only through the Source API.
"""

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import httpx
import psycopg2
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from cdc_stream import CdcStream
from warehouse import Warehouse

DB_CFG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
    "dbname":   os.getenv("PG_DB", "enterprise_db"),
}
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
CONNECTOR_NAME = "postgres-connector"
CONNECTOR_CONFIG = Path(os.getenv("CONNECTOR_CONFIG", "../../debezium/postgres-connector.json"))
# Row counts shown in the pipeline bar
SOURCE_TABLES = ["customers", "shops", "product_skus", "checkouts", "orders"]
SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://localhost:8200")
SOURCE_API_URL = os.getenv("SOURCE_API_URL", "http://localhost:8100")

simulator = httpx.AsyncClient(base_url=SIMULATOR_URL, timeout=60)
stream = CdcStream(KAFKA_BROKER, ["^cdc\\.ecommerce\\..*"])   # regex: every table topic
warehouse = Warehouse(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    user=os.getenv("CLICKHOUSE_USER", "admin"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "admin"),
)

app = FastAPI(title="CDC Control Center")


@app.on_event("startup")
def startup():
    stream.start()


# ---------------------------------------------------------------------------
# Pipeline health
# ---------------------------------------------------------------------------

def _debezium_status() -> dict:
    try:
        with urllib.request.urlopen(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status", timeout=3) as r:
            body = json.load(r)
        tasks = body.get("tasks", [])
        state = body["connector"]["state"]
        if any(t.get("state") != "RUNNING" for t in tasks):
            state = "DEGRADED"
        return {"up": True, "state": state}
    except urllib.error.HTTPError as e:
        return {"up": True, "state": "NOT_REGISTERED" if e.code == 404 else f"HTTP_{e.code}"}
    except Exception:
        return {"up": False, "state": "UNREACHABLE"}


@app.get("/api/health")
def health():
    source = {"up": False, "counts": {}}
    try:
        with psycopg2.connect(connect_timeout=3, **DB_CFG) as conn, conn.cursor() as cur:
            for t in SOURCE_TABLES:
                cur.execute(f"SELECT count(*) FROM ecommerce.{t}")
                source["counts"][t] = cur.fetchone()[0]
            cur.execute(
                "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0) "
                "FROM pg_replication_slots WHERE slot_name = 'debezium_slot'"
            )
            row = cur.fetchone()
            source["slot_lag_bytes"] = int(row[0]) if row else None
        source["up"] = True
    except Exception as exc:
        source["error"] = str(exc)

    clickhouse = {"up": warehouse.ping()}
    if clickhouse["up"]:
        try:
            clickhouse["bronze_rows"] = warehouse.rows("SELECT count() AS n FROM bronze.cdc_events")[0]["n"]
        except Exception as exc:
            clickhouse["error"] = str(exc)

    sim = {"up": False, "running": False}
    try:
        s = httpx.get(f"{SIMULATOR_URL}/status", params={"log_since": 1 << 62}, timeout=3).json()
        sim = {"up": True, "running": s["running"], "seeding": s["seeding"]}
    except (httpx.HTTPError, ValueError):
        pass

    try:
        source_api = {"up": httpx.get(f"{SOURCE_API_URL}/health", timeout=3).status_code == 200}
    except httpx.HTTPError:
        source_api = {"up": False}

    return {
        "postgres":   source,
        "debezium":   _debezium_status(),
        "kafka":      {"up": stream.connected, "rate": stream.snapshot(stream.seq)["rate"]},
        "clickhouse": clickhouse,
        "generator":  sim,
        "source_api": source_api,
    }


@app.post("/api/connector/register")
def register_connector():
    body = CONNECTOR_CONFIG.read_bytes()
    req = urllib.request.Request(
        f"{DEBEZIUM_URL}/connectors", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except urllib.error.HTTPError as e:
        if e.code != 409:  # 409 = already exists
            raise HTTPException(502, f"Debezium returned {e.code}: {e.read().decode()[:300]}")
    return _debezium_status()


# ---------------------------------------------------------------------------
# Simulator (proxied)
# ---------------------------------------------------------------------------

async def _proxy(method: str, path: str, request: Request | None = None, params: dict | None = None):
    body = await request.body() if request is not None else None
    try:
        r = await simulator.request(method, path, params=params, content=body or None,
                              headers={"Content-Type": "application/json"} if body else None)
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"Simulator unavailable: {exc}")
    return JSONResponse(r.json(), status_code=r.status_code)


@app.get("/api/generator")
async def generator_status(log_since: int = 0):
    return await _proxy("GET", "/status", params={"log_since": log_since})


@app.post("/api/generator/start")
async def generator_start(request: Request):
    return await _proxy("POST", "/start", request)


@app.post("/api/generator/stop")
async def generator_stop():
    return await _proxy("POST", "/stop")


@app.put("/api/generator/config")
async def generator_config(request: Request):
    return await _proxy("PUT", "/config", request)


@app.post("/api/actions/{name}")
async def run_action(name: str, request: Request):
    return await _proxy("POST", f"/actions/{name}", request)


@app.put("/api/clock")
async def set_clock(request: Request):
    return await _proxy("PUT", "/clock", request)


@app.post("/api/generator/seed")
async def generator_seed(request: Request):
    return await _proxy("POST", "/seed", request)


# ---------------------------------------------------------------------------
# Live CDC + warehouse
# ---------------------------------------------------------------------------

@app.get("/api/cdc")
def cdc(since: int = 0):
    return stream.snapshot(since)


@app.get("/api/warehouse")
def warehouse_overview():
    try:
        return warehouse.overview()
    except Exception as exc:
        raise HTTPException(503, f"ClickHouse unavailable: {exc}")


# Built React app (present in the Docker image)
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="ui")
