#!/bin/bash
set -euo pipefail

if [ -z "$(ls -A "${PGDATA}" 2>/dev/null)" ]; then
  echo "[standby] empty data dir - cloning ${PRIMARY_HOST}:${PRIMARY_PORT} via pg_basebackup"
  export PGPASSWORD="${POSTGRES_REPLICATION_PASSWORD}"

  attempt=1
  max_attempts=10
  until gosu postgres pg_basebackup \
    -h "${PRIMARY_HOST}" -p "${PRIMARY_PORT}" \
    -U "${POSTGRES_REPLICATION_USER}" \
    -D "${PGDATA}" -Fp -Xs -P -R; do
    if [ "${attempt}" -ge "${max_attempts}" ]; then
      echo "[standby] pg_basebackup failed after ${max_attempts} attempts, giving up"
      exit 1
    fi
    echo "[standby] pg_basebackup attempt ${attempt}/${max_attempts} failed, primary may still be" \
      "restarting after initdb; retrying in 3s"
    rm -rf "${PGDATA:?}"/*
    attempt=$((attempt + 1))
    sleep 3
  done
fi

exec docker-entrypoint.sh postgres
