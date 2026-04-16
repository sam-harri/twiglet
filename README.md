<p align="center">
  <img src="https://raw.githubusercontent.com/sam-harri/docs/refs/heads/master/twiglet.png"/>
</p>

A control plane in front of regular object storage adding branching, copy-on-write, continous point-in-time recovery, and time travel reads.


## Running Locally

Requires Docker and Cargo.

```bash
./scripts/dev.sh
```

This starts MinIO, exports env vars, and runs the control plane.

All config is with env vars :

| Variable | Default | Description |
|---|---|---|
| `TWIGLET_PORT` | `8080` | Listen port |
| `TWIGLET_CHUNK_STORE_TYPE` | `local` | `local` or `s3` |
| `TWIGLET_STORAGE_BUCKET` | `twiglet-chunks` | Bucket/directory for chunks |
| `TWIGLET_STORAGE_ENDPOINT` | — | S3 endpoint (for MinIO or compatible) |
| `TWIGLET_STORAGE_ACCESS_KEY_ID` | — | S3 access key |
| `TWIGLET_STORAGE_SECRET_ACCESS_KEY` | — | S3 secret key |
| `TWIGLET_ROCKSDB_PATH` | `./data/rocksdb` | Path for metadata storage |
| `TWIGLET_ADMIN_USERNAME` | `twigletadmin` | Basic auth username |
| `TWIGLET_ADMIN_PASSWORD` | `twigletadmin` | Basic auth password |


# Clients

Python and TS clients generated from the OpenAPI spec. New Clients can be generated using `./scripts/gen-clients.sh` when the OpenAPI spec is updated.

- npm install /twiglet-engine/clients/typescript
- uv install -e /twiglet-engine/clients/python

# Benchmarks

Turn criterion benchmark results in a CSV file

(echo "depth,mean_us,lower_us,upper_us" && for d in 1 2 3 4 5 6 7 8; do jq -r --arg d "$d" '[$d, (.slope.point_estimate/1000), (.slope.confidence_interval.lower_bound/1000), (.slope.confidence_interval.upper_bound/1000)] | @csv' target/criterion/ancestry_depth/$d/new/estimates.json; done) > ancestry_depth.csv

(echo "size_bytes,mean_gibibytes_s,lower_gibibytes_s,upper_gibibytes_s,mean_req_s,lower_req_s,upper_req_s" && for s in 1024 16384 65536 262144 1048576 4194304 8388608 16777216 33554432 67108864; do jq -r --arg s "$s" '($s | tonumber) as $sz | (.slope // .mean) | [$s, ($sz * 1e9 / (.point_estimate * 1073741824)), ($sz * 1e9 / (.confidence_interval.upper_bound * 1073741824)), ($sz * 1e9 / (.confidence_interval.lower_bound * 1073741824)), (1e9 / .point_estimate), (1e9 / .confidence_interval.upper_bound), (1e9 / .confidence_interval.lower_bound)] | @csv' target/criterion/write_throughput/$s/new/estimates.json; done) > write_throughput.csv

(echo "dataset_size,mean_us,lower_us,upper_us" && for n in 100 1000 10000 100000; do jq -r --arg n "$n" '(.slope // .mean) | [$n, (.point_estimate/1000), (.confidence_interval.lower_bound/1000), (.confidence_interval.upper_bound/1000)] | @csv' target/criterion/fork_cost/$n/new/estimates.json; done) > fork_cost.csv

