<p align="center">
  <img src="https://raw.githubusercontent.com/sam-harri/docs/refs/heads/master/twiglet.png"/>
</p>

A control plane in front of regular object storage adding branching, copy-on-write, continous point-in-time recovery, and time travel reads.


## Running Locally

Requires Docker and Cargo.

```bash
./scripts/dev.sh
```

This starts MinIO, exports env vars, and runs the engine.

## Configuration

All config is via environment variables prefixed with `TWIGLET_`. Groups use `__` as a separator (e.g. `TWIGLET_SERVER__PORT=9090`).

### Server

| Variable | Default | |
|---|---|---|
| `TWIGLET_SERVER__HOST` | `0.0.0.0` | Bind address |
| `TWIGLET_SERVER__PORT` | `8080` | Listen port |

### Auth

| Variable | Default | |
|---|---|---|
| `TWIGLET_AUTH__USERNAME` | `twigletadmin` | Basic auth username |
| `TWIGLET_AUTH__PASSWORD` | `twigletadmin` | Basic auth password |

### Metastore

Set `TWIGLET_METASTORE__TYPE` to select a backend (default: `rocksdb`).

**`rocksdb`**

| Variable | Default | |
|---|---|---|
| `TWIGLET_METASTORE__PATH` | `./data/rocksdb` | Database directory |

### Chunker

Set `TWIGLET_CHUNKER__TYPE` to select an algorithm (default: `fixed`).

**`fixed`** — splits data into equal-sized blocks

| Variable | Default | |
|---|---|---|
| `TWIGLET_CHUNKER__CHUNK_SIZE_BYTES` | `8388608` | Block size in bytes (default 8 MiB) |

**`cdc`** — content-defined chunking

| Variable | Default | |
|---|---|---|
| `TWIGLET_CHUNKER__MIN_CHUNK_BYTES` | — | Minimum chunk size |
| `TWIGLET_CHUNKER__AVG_CHUNK_BYTES` | — | Target average chunk size |
| `TWIGLET_CHUNKER__MAX_CHUNK_BYTES` | — | Maximum chunk size |

### Storage

Set `TWIGLET_STORAGE__TYPE` to select a backend (default: `local`).

**`local`** — writes chunks to the local filesystem

| Variable | Default | |
|---|---|---|
| `TWIGLET_STORAGE__PATH` | `twiglet-chunks` | Directory for chunk files |

**`s3`** — AWS S3 or any S3-compatible store (MinIO, R2, etc.)

| Variable | Default | |
|---|---|---|
| `TWIGLET_STORAGE__BUCKET` | — | Bucket name (required) |
| `TWIGLET_STORAGE__REGION` | — | region |
| `TWIGLET_STORAGE__ENDPOINT` | — | Custom endpoint URL |
| `TWIGLET_STORAGE__ACCESS_KEY_ID` | — | Access key |
| `TWIGLET_STORAGE__SECRET_ACCESS_KEY` | — | Secret key |


# Clients

Python and TS clients generated from the OpenAPI spec. New Clients can be generated using `./scripts/gen-clients.sh` when the OpenAPI spec is updated.

- npm install /twiglet-engine/clients/typescript
- uv install -e /twiglet-engine/clients/python

# Benchmarks

Turn criterion benchmark results into a CSV file

```bash
(echo "depth,mean_us,lower_us,upper_us" && for d in 1 2 3 4 5 6 7 8; do jq -r --arg d "$d" '[$d, (.slope.point_estimate/1000), (.slope.confidence_interval.lower_bound/1000), (.slope.confidence_interval.upper_bound/1000)] | @csv' target/criterion/ancestry_depth/$d/new/estimates.json; done) > ancestry_depth.csv

(echo "size_bytes,mean_gibibytes_s,lower_gibibytes_s,upper_gibibytes_s,mean_req_s,lower_req_s,upper_req_s" && for s in 1024 16384 65536 262144 1048576 4194304 8388608 16777216 33554432 67108864; do jq -r --arg s "$s" '($s | tonumber) as $sz | (.slope // .mean) | [$s, ($sz * 1e9 / (.point_estimate * 1073741824)), ($sz * 1e9 / (.confidence_interval.upper_bound * 1073741824)), ($sz * 1e9 / (.confidence_interval.lower_bound * 1073741824)), (1e9 / .point_estimate), (1e9 / .confidence_interval.upper_bound), (1e9 / .confidence_interval.lower_bound)] | @csv' target/criterion/write_throughput/$s/new/estimates.json; done) > write_throughput.csv

(echo "dataset_size,mean_us,lower_us,upper_us" && for n in 100 1000 10000 100000; do jq -r --arg n "$n" '(.slope // .mean) | [$n, (.point_estimate/1000), (.confidence_interval.lower_bound/1000), (.confidence_interval.upper_bound/1000)] | @csv' target/criterion/fork_cost/$n/new/estimates.json; done) > fork_cost.csv
```
