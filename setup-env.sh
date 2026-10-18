#!/usr/bin/env bash
# Creates .env from .env.example (every change-me becomes a random value) and the secrets/ directory: one file per
# secret, mounted read-only into the services that need it under /run/secrets. Existing values are never replaced,
# so it is safe to run again after pulling: it only adds what is missing.
set -euo pipefail
cd "$(dirname "$0")"
umask 077
rand() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

if [[ ! -e .env ]]; then
  while IFS= read -r line; do
    [[ "$line" == *=change-me ]] && line="${line%change-me}$(rand)"
    printf '%s\n' "$line"
  done < .env.example > .env
  echo "Wrote .env"
else                                    # add keys introduced since .env was written
  while IFS= read -r line; do
    [[ "$line" == *=change-me ]] || continue
    key="${line%%=*}"
    grep -q "^$key=" .env || { printf '%s=%s\n' "$key" "$(rand)" >> .env; echo "Added $key to .env"; }
  done < .env.example
fi

mkdir -p secrets/clients secrets/webhooks
# identity service: password pepper, the admin login, service-client secrets; partners: webhook signing keys
for f in password_pepper admin_password clients/sim-orchestrator clients/quality clients/identity \
         webhooks/MOMO webhooks/VNPAY webhooks/NAPAS webhooks/GHN webhooks/GHTK webhooks/SPX webhooks/JT webhooks/VTP; do
  [[ -s "secrets/$f" ]] || { rand > "secrets/$f"; echo "Wrote secrets/$f"; }
done
# readable by the container users (the files stay private to this machine: secrets/ is gitignored)
chmod 755 secrets secrets/clients secrets/webhooks
find secrets -type f -exec chmod 644 {} +
