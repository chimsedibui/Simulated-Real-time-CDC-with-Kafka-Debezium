#!/usr/bin/env bash
# Creates the secrets/ directory (gitignored): one file per secret, mounted read-only into the services that need
# it under /run/secrets, so no password appears in `docker inspect` or the compose file. Also writes .env (the
# non-secret settings, from .env.example). Existing values are never replaced: running it again only adds what is
# missing. Values still in an older .env are moved into secrets/ (and removed from .env), so a running stack keeps
# its passwords.
#
#   ./setup-env.sh            create what is missing
#   ./setup-env.sh --rotate   new random values for every secret (the TLS CA and certificate are kept). Databases
#                             set their passwords on first start only, so rotate with the stack down and its
#                             volumes removed (a data reset); the new PII salt also changes every hashed value.
set -euo pipefail
cd "$(dirname "$0")"
umask 077
rand() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

[[ "${1:-}" == "" || "${1:-}" == "--rotate" ]] || { echo "usage: $0 [--rotate]" >&2; exit 2; }
if [[ "${1:-}" == "--rotate" ]]; then
  if [[ -n "$(docker ps -q --filter label=com.docker.compose.project=realtime-cdc 2>/dev/null)" ]]; then
    echo "stop the stack and remove its volumes first: docker compose --profile tools down -v" >&2; exit 1
  fi
  if [[ -d source/postgres/data || -n "$(docker volume ls -q --filter label=com.docker.compose.project=realtime-cdc)" ]]; then
    echo "rotation requires a data reset: remove project volumes and source/postgres/data first; existing database credentials must not be overwritten" >&2; exit 1
  fi
  find secrets -type f ! -path 'secrets/tls/*' -delete 2>/dev/null || true
  [[ -f .env ]] && sed -i -E '/^[A-Z_]*(PASSWORD|SALT|KEY)=/d' .env
  echo "Removed the old secrets; writing new ones"
fi

SECRETS=(POSTGRES_PASSWORD CDC_DB_PASSWORD SOURCE_API_DB_PASSWORD APP_READER_PASSWORD IDENTITY_DB_PASSWORD
         MONITOR_PASSWORD CLICKHOUSE_PASSWORD GRAFANA_READER_PASSWORD GRAFANA_ADMIN_PASSWORD PII_SALT
         COLD_STORAGE_ACCESS_KEY COLD_STORAGE_SECRET_KEY)
mkdir -p secrets/clients secrets/webhooks
for key in "${SECRETS[@]}"; do
  file="secrets/$(echo "$key" | tr 'A-Z' 'a-z')"
  [[ -s "$file" ]] && continue
  old=$( [[ -f .env ]] && sed -n "s/^$key=//p" .env | tail -1 || true)
  printf '%s' "${old:-$(rand)}" > "$file"           # no trailing newline: Debezium reads the file verbatim
  echo "Wrote $file${old:+ (moved from .env)}"
done
if [[ -f .env ]]; then                               # secrets now live in files only
  pattern=$(IFS='|'; echo "^(${SECRETS[*]})=")
  grep -Ev "$pattern" .env > .env.tmp || true
  mv .env.tmp .env
else
  cp .env.example .env
  echo "Wrote .env"
fi
grep -q '^AUTH_MODE=' .env || sed -n 's/^AUTH_MODE=/AUTH_MODE=/p' .env.example >> .env

# identity service: password pepper, the admin login, service-client secrets; partners: webhook signing keys
for f in password_pepper admin_password clients/sim-orchestrator clients/quality clients/identity \
         webhooks/MOMO webhooks/VNPAY webhooks/NAPAS webhooks/GHN webhooks/GHTK webhooks/SPX webhooks/JT webhooks/VTP; do
  [[ -s "secrets/$f" ]] || { rand > "secrets/$f"; echo "Wrote secrets/$f"; }
done
# local CA and a certificate for the gateway (api.cdc.localhost, auth.cdc.localhost); trust secrets/tls/ca.crt
if [[ ! -s secrets/tls/server.crt ]]; then
  mkdir -p secrets/tls
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=CDC local CA" \
    -keyout secrets/tls/ca.key -out secrets/tls/ca.crt 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=cdc.localhost" \
    -keyout secrets/tls/server.key -out secrets/tls/server.csr 2>/dev/null
  printf 'subjectAltName=DNS:api.cdc.localhost,DNS:auth.cdc.localhost,DNS:app.cdc.localhost,DNS:localhost\nextendedKeyUsage=serverAuth\n' \
    > secrets/tls/server.ext
  openssl x509 -req -in secrets/tls/server.csr -CA secrets/tls/ca.crt -CAkey secrets/tls/ca.key -CAcreateserial \
    -days 825 -extfile secrets/tls/server.ext -out secrets/tls/server.crt 2>/dev/null
  rm -f secrets/tls/server.csr secrets/tls/server.ext
  echo "Wrote secrets/tls (trust secrets/tls/ca.crt to call https://api.cdc.localhost:8443 without -k)"
fi
# readable by the container users (the files stay private to this machine: secrets/ is gitignored)
chmod 755 secrets secrets/clients secrets/webhooks secrets/tls 2>/dev/null || true
find secrets -type f ! -name ca.key -exec chmod 644 {} +   # the CA key stays private
