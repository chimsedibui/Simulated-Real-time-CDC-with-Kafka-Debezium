"""Source API — the only writer to the operational PostgreSQL database.

Every endpoint that changes data is one business command: it runs in a single
transaction, writes a domain event to ecommerce.outbox_events, and accepts an
Idempotency-Key header so clients (and webhook senders) can retry safely.
"""

import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import close_pool, init_pool, transaction
from .jobs import start_scheduler
from .routers import admin, carts, checkouts, customers, logistics, orders, returns, sellers


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    stop = threading.Event()
    if os.getenv("RUN_JOBS", "true").lower() == "true":
        start_scheduler(stop)
    yield
    stop.set()
    close_pool()


app = FastAPI(title="Marketplace Source API", version="0.3.0", lifespan=lifespan)
for module in (admin, sellers, customers, carts, checkouts, orders, logistics, returns):
    app.include_router(module.router)


@app.get("/health")
def health():
    with transaction() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}
