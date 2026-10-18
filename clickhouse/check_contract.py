"""Data contract check: PostgreSQL schema vs the warehouse spec (gen_silver.py) and the Debezium connector.

Fails when a source change would otherwise reach the warehouse silently wrong:
  - a streamed table without a silver spec, or a spec table missing at the source
  - a source column that is neither in the spec nor listed in NOT_IN_WAREHOUSE (new column forgotten)
  - a spec column missing at the source, or with an incompatible type / nullability
  - a hashed PII column that is not a string in the spec
  - a state table without REPLICA IDENTITY FULL (consistent.* needs the before image)
  - clickhouse/init/02-silver.sql not regenerated after a spec change

    PG_HOST=localhost PG_PORT=15432 python3 clickhouse/check_contract.py
Exit code 0 = contract holds. Runs in CI against a fresh database built from postgres/init.sql.
"""

import json
import os
import re
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from gen_silver import TABLES, render  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CONNECTOR = json.loads((ROOT / "debezium" / "postgres-connector.json").read_text())["config"]

# Source columns deliberately kept out of the warehouse, with the reason
NOT_IN_WAREHOUSE = {
    "outbox_events": "routed to events.* by the Outbox Event Router, lands in bronze.domain_events",
    "debezium_signals": "Debezium control table",
}

SPEC_TYPES = {
    "U": {"uuid"},
    "S": {"character varying", "text", "character"},
    "L": {"character varying", "text", "character"},
    "I": {"bigint"},
    "i": {"integer", "smallint"},
    "B": {"boolean"},
    "T": {"timestamp with time zone"},
}


def streamed(table: str) -> bool:
    return re.fullmatch(CONNECTOR["table.include.list"], f"ecommerce.{table}") is not None


def main():
    pg = psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")),
                          user=os.getenv("PG_USER", "admin"), password=os.getenv("PG_PASSWORD", "admin"),
                          dbname=os.getenv("PG_DB", "enterprise_db"))
    with pg.cursor() as cur:
        cur.execute("""SELECT table_name, column_name, data_type, is_nullable = 'YES'
                       FROM information_schema.columns c
                       JOIN information_schema.tables t USING (table_schema, table_name)
                       WHERE table_schema = 'ecommerce' AND t.table_type = 'BASE TABLE'
                       ORDER BY table_name, ordinal_position""")
        source = {}
        for table, column, data_type, nullable in cur.fetchall():
            source.setdefault(table, {})[column] = (data_type, nullable)
        cur.execute("""SELECT c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname = 'ecommerce' AND c.relkind = 'r'""")
        identity = dict(cur.fetchall())

    problems = []
    masked = {c.split(".", 1)[1] for k, v in CONNECTOR.items() if k.startswith("column.mask.hash") for c in v.split(",")}

    for table in sorted(t for t in source if streamed(t) and t not in NOT_IN_WAREHOUSE):
        if table not in TABLES:
            problems.append(f"{table}: streamed by Debezium but has no silver spec")
    for table, (kind, _key, spec) in TABLES.items():
        if table not in source:
            problems.append(f"{table}: in the silver spec but not in PostgreSQL")
            continue
        if not streamed(table):
            problems.append(f"{table}: in the silver spec but excluded by table.include.list")
        cols = dict(c.strip().split() for c in spec.split(","))
        for column, code in cols.items():
            if column not in source[table]:
                problems.append(f"{table}.{column}: in the spec but not in PostgreSQL")
                continue
            data_type, nullable = source[table][column]
            if data_type not in SPEC_TYPES[code.rstrip("?")]:
                problems.append(f"{table}.{column}: PostgreSQL {data_type} vs spec '{code}'")
            if nullable and not code.endswith("?"):
                problems.append(f"{table}.{column}: nullable in PostgreSQL, NOT NULL in the spec (nulls would become 0/'')")
            if f"{table}.{column}" in masked and code.rstrip("?") not in "SL":
                problems.append(f"{table}.{column}: hashed by Debezium, so the spec must declare a string")
        for column in source[table]:
            if column not in cols:
                problems.append(f"{table}.{column}: new source column not in the silver spec (add it, or list why not)")
        if kind == "state" and identity.get(table) != "f":
            problems.append(f"{table}: state table needs REPLICA IDENTITY FULL (set in postgres/init.sql)")

    generated = (ROOT / "clickhouse" / "init" / "02-silver.sql").read_text()
    if generated != render():
        problems.append("clickhouse/init/02-silver.sql is stale: run python3 clickhouse/gen_silver.py")

    for p in problems:
        print(f"[FAIL] {p}")
    print(f"{len(TABLES)} tables, {sum(len(c) for c in source.values())} source columns checked: "
          f"{'contract holds' if not problems else f'{len(problems)} problems'}")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
