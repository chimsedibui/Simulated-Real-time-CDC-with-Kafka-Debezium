#!/usr/bin/env bash
# Creates or updates the Debezium connector from postgres-connector.json, filling in PII_SALT from .env
# (the salt is part of a config key, which Connect's config providers can't fill in).
#   debezium/register.sh            # Kafka Connect at localhost:8083, or set DEBEZIUM_URL
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then set -a; . ./.env; set +a; fi
: "${PII_SALT:?set it in .env, see .env.example}"
python3 - "${DEBEZIUM_URL:-http://localhost:8083}" <<'PY'
import json, os, sys, urllib.request
spec = json.loads(open("debezium/postgres-connector.json").read().replace("${PII_SALT}", os.environ["PII_SALT"]))
req = urllib.request.Request(f"{sys.argv[1]}/connectors/{spec['name']}/config", json.dumps(spec["config"]).encode(),
                             {"Content-Type": "application/json"}, method="PUT")
with urllib.request.urlopen(req, timeout=30) as response:
    print(f"connector {spec['name']}: HTTP {response.status}")
PY
