#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker-compose.minio.yml up -d minio minio-setup
trap 'docker compose -f docker-compose.minio.yml down' EXIT

export TWIGLET_CHUNK_STORE_TYPE=s3
export TWIGLET_STORAGE_BUCKET=twiglet-chunks
export TWIGLET_STORAGE_REGION=local
export TWIGLET_STORAGE_ENDPOINT=http://localhost:9000
export TWIGLET_STORAGE_ACCESS_KEY_ID=minioadmin
export TWIGLET_STORAGE_SECRET_ACCESS_KEY=minioadmin
export TWIGLET_HOST=127.0.0.1

export RUST_LOG=info

cargo run
