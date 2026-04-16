#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker-compose.minio.yml up -d minio minio-setup
trap 'docker compose -f docker-compose.minio.yml down' EXIT

export TWIGLET_STORAGE__TYPE=s3
export TWIGLET_STORAGE__BUCKET=twiglet-chunks
export TWIGLET_STORAGE__ENDPOINT=http://localhost:9000
export TWIGLET_STORAGE__ACCESS_KEY_ID=minioadmin
export TWIGLET_STORAGE__SECRET_ACCESS_KEY=minioadmin

export RUST_LOG=info

cargo run
