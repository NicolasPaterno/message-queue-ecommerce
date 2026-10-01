#!/bin/sh
# Generates everything kept out of git: a CA + server cert for TLS, random broker passwords (.env),
# definitions.json rendered from definitions.tmpl.json, and web/.env.local. Existing files are kept.
set -eu
cd "$(dirname "$0")"

mkdir -p certs
if [ ! -f certs/server.pem ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=shop-ca" \
    -keyout certs/ca.key -out certs/ca.pem 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=rabbitmq" \
    -keyout certs/server.key -out certs/server.csr 2>/dev/null
  printf 'subjectAltName=DNS:rabbitmq,DNS:localhost\n' > certs/san.ext
  openssl x509 -req -in certs/server.csr -CA certs/ca.pem -CAkey certs/ca.key -CAcreateserial \
    -days 365 -extfile certs/san.ext -out certs/server.pem 2>/dev/null
  rm certs/server.csr certs/san.ext
  # the broker runs as uid 999 inside the container and must read the key from the bind mount
  chmod 644 certs/server.key
fi

if [ ! -f .env ]; then
  for u in API WORKER MONITOR ADMIN; do echo "${u}_PASS=$(openssl rand -hex 16)"; done > .env
fi
. ./.env

sed -e "s/__API_PASS__/$API_PASS/" -e "s/__WORKER_PASS__/$WORKER_PASS/" \
    -e "s/__MONITOR_PASS__/$MONITOR_PASS/" -e "s/__ADMIN_PASS__/$ADMIN_PASS/" \
    definitions.tmpl.json > definitions.json

printf 'MQ_URL=https://localhost:15671\nMQ_USER=monitor\nMQ_PASS=%s\n' "$MONITOR_PASS" > ../web/.env.local
echo "ok: certs/, .env, definitions.json, ../web/.env.local"
