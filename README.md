# Bucket

Durable, replicated **S3-subset** object storage behind a tiny HTTP server.

This repository is intentionally small:

- **S3 compatibility is a strict subset** (path-style only).
- **Durability is on** (WAL + commit files per replica under `--data-dir`).
- The “distributed” part is an **in-process replica cluster** (quorum replication), plus
  **deterministic distributed simulation tests** (vopr-style) to harden the core.

If you need drop-in S3 (SigV4/TLS/multipart/range/strict XML compatibility), this repo is **not** that.

---

## Build

```sh
zig build
```

## Test

```sh
zig build test --summary all
```

The test suite is the source of truth (no hard-coded “green” claims in docs). See:

- `docs/TEST_MATRIX.md`
- `docs/ERROR_TAXONOMY.md`

---

## Run

```sh
zig build run -- --data-dir /tmp/s3data --replicas 3
```

Defaults:

- listens on `127.0.0.1:9000` (safer default)
- `--replicas` defaults to `3`
- `--data-dir` defaults to `.zig-cache/s3bucket`

Optional:

- `--bind <ip>` (e.g. `0.0.0.0`)
- `--port <port>`

---

## Supported HTTP API (Compatibility Envelope)

Path-style only:

- `PUT /<bucket>`
- `DELETE /<bucket>`
- `GET /` (ListBuckets)
- `PUT /<bucket>/<key>` (PutObject, **max body 16MiB**)
- `GET /<bucket>/<key>` (GetObject)
- `HEAD /<bucket>/<key>` (HeadObject)
- `DELETE /<bucket>/<key>` (DeleteObject)
- `GET /<bucket>?list-type=2` (ListObjectsV2)

### Envelope details (S3-like behaviors)

- Bucket names are validated to be **S3/DNS-ish** (lowercase, 3..63 chars, `a-z0-9.-`, no `..`, not IPv4-like).
- Object keys are **URL percent-decoded** (`%20` becomes a space, `%2F` becomes `/` inside a key).
- `ListObjectsV2` returns keys in **lexicographic order**.
- `DeleteObject` is **idempotent** (missing key => `204 No Content`).
- `HEAD` returns the **same Content-Length as GET** (but no body).

### Examples

```sh
# Create bucket
curl -H 'Content-Length: 0' -X PUT http://127.0.0.1:9000/mybucket

# Put object
curl -X PUT --data-binary 'hello' http://127.0.0.1:9000/mybucket/key1

# Get object
curl http://127.0.0.1:9000/mybucket/key1

# List objects
curl "http://127.0.0.1:9000/mybucket?list-type=2"
```

---

## Non-goals

- TLS
- SigV4 / authentication
- Multipart upload
- Range requests
- Streaming large objects (current max request body is 16MiB)
- Exact XML / header compatibility with AWS S3 for all clients
- Multi-host networking / real cluster membership
