#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cargo run --bin generate_openapi --manifest-path "$ROOT/Cargo.toml" > "$ROOT/openapi.json"
trap 'rm openapi.json' EXIT

uvx openapi-python-client generate \
  --path "$ROOT/openapi.json" \
  --output-path "$ROOT/clients/python" \
  --overwrite

npx --yes @hey-api/openapi-ts \
  --input "$ROOT/openapi.json" \
  --output "$ROOT/clients/typescript/src" \
  --client @hey-api/client-fetch