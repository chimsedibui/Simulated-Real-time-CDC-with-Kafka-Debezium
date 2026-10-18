#!/usr/bin/env bash
# Creates .env from .env.example, replacing every change-me with a random secret. Never overwrites .env.
set -euo pipefail
cd "$(dirname "$0")"
if [[ -e .env ]]; then
  echo ".env already exists; edit it by hand" >&2
  exit 1
fi
umask 077
while IFS= read -r line; do
  if [[ "$line" == *=change-me ]]; then
    line="${line%change-me}$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
  printf '%s\n' "$line"
done < .env.example > .env
echo "Wrote .env"
