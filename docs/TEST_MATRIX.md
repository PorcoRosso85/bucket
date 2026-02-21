# Test matrix (what is covered, and how)

## Unit tests (S3 subset)

- `src/urn__app__test__all.zig`
  - bucket create/list/delete
  - put/get/delete object
  - HEAD content-length (HTTP層でbodyが省略されても長さは正しい)
  - DeleteObject idempotency (missing key => 204)
  - URL percent-decoding for keys (`%20` => space, `%2F` => '/' inside key)
  - ListObjectsV2 lexicographic order
  - bucket name validation (reject invalid names)
  - bucket name validation boundary cases (length 3..63, '..' reject, IPv4-like reject, shape/charset)
  - error mapping: `NoSuchBucket`, `NoSuchKey`, `BucketAlreadyExists`, `BucketNotEmpty`

## Durability (restart)

- `src/urn__app__test__all.zig`
  - `Durability: restart preserves committed state`

## HTTP end-to-end (S3 subset)

- `src/urn__app__test__http_e2e.zig`
  - `http_e2e: basic S3 subset over HTTP`
    - bucket create/delete
    - object put/get/head/delete
    - ListObjectsV2 (`?list-type=2`)
    - regression: **PUT object with body must not crash/panic**
    - regression: HEAD must not block the client (body is elided)
    - regression: DeleteObject must be idempotent
  - `http_e2e: durability across restart (HTTP)`
    - create bucket/object, restart store/server, read back
  - `http_e2e: compatibility edges (URL decode, list order, bucket name validation)`
    - URL percent-decoding for object keys
      - includes `%2F` (slash inside key) over HTTP
    - lexicographic ListObjectsV2 ordering
    - InvalidBucketName rejection
    - XML escaping for keys (e.g. `& < > " '` must not break ListObjectsV2 XML)

  - `http_e2e: errors + limits (Compatibility Envelope)`
    - invalid percent-encoding in the path returns `InvalidRequest` (400)

## Process-level blackbox (separate process)

- `src/urn__app__test__process_blackbox.zig`
  - spawns `zig-out/bin/s3gw` and drives it over real sockets
  - covers accept loop + multi-connection (sequential) + slow-body chunking

## Distributed simulation tests (vopr-style)

- `src/urn__app__test__dist_vopr.zig`

  - `dist/vopr determinism (same seed => same trace hash)`
    - 同じ seed / 同じ fault profile で **完全に同じ trace hash** になること
    - `oracle_ok == true`

  - `dist/vopr fault matrix (safety + convergence)`
    - fault profile を切り替えながら seed=1..10 を走らせる
    - 期待: `oracle_ok == true`

### Fault domains covered by the matrix

- Network
  - drop / dup / delay
  - corrupt / misdirect
- Process
  - pause/resume
  - crash/recover (**leader を含む**)
- Partition
  - 1<->2 を partition/heal
- Disk
  - follower write corrupt/torn
  - scrub_corrupt (後から破損)
  - fsync delay (`disk_fsync_max_delay`) による **未fsync領域のcrash消失**

### Oracle conditions

- Safety
  - `commit_index <= durable_last_index`
  - `applied_index <= commit_index`
  - `commit_index >= snapshot_index`
  - (粗いチェック) 稼働中に leader は高々1つ

- Convergence (stabilizeフェーズ)
  - fault注入を0にし resync を強制
  - 全稼働ノードで
    - `commit_index == leader.commit_index`
    - `applied_index == commit_index`
    - `storeDigest(node) == storeDigest(leader)`

## What is NOT covered yet (explicit gaps)

- Membership changes (add/remove node)
- Real sockets / multi-process
- OS filesystem durability semantics (rename/partial write ordering) の完全モデル
- Full S3 compatibility (SigV4, multipart, range, streaming)

## Production store DST (vopr-style)

- `src/urn__app__test__store_dist_vopr.zig`
  - determinism: same seed => same final digest
  - fault matrix (seeds 1..10): replica crash/recover (including leader) + follower WAL corruption
  - convergence: after heal write, all replicas match leader digest
  - durability: reopen from disk, leader digest matches
