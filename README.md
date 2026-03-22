# Durable LSM-Style Key-Value Store (Go)

Simple HTTP key-value service with a concurrency-safe MemTable, a write-ahead log (WAL), and immutable SSTables on disk.

## Features

- `PUT /kv/{key}` creates or overwrites a value.
- `GET /kv/{key}` returns the latest value bytes.
- `DELETE /kv/{key}` removes a key.
- `GET /health` returns a health payload.
- Thread-safe map store using `sync.RWMutex`.
- WAL durability with replay on startup.
- SSTable flush when the MemTable reaches a size threshold.
- Full in-memory SSTable index plus min/max key metadata.
- Tombstones for deletes.
- Unit tests, HTTP integration tests, flush tests, recovery tests, and race checks.

## Durability Contract

- On `PUT` and `DELETE`, the server appends the mutation to the WAL before mutating the in-memory map.
- If WAL append fails, the operation fails and the in-memory state is left unchanged.
- With `WAL_SYNC_MODE=always`, acknowledged writes survive process or machine crashes once the response has been returned.
- With `WAL_SYNC_MODE=never`, recently acknowledged writes may be lost after a crash, but state is still rebuilt from the valid WAL prefix on restart.

## LSM Storage Model

- New writes land in the active MemTable and WAL first.
- When the MemTable reaches the configured threshold, it is sorted and flushed to a new immutable SSTable file.
- Reads check the active MemTable first, then SSTables from newest to oldest.
- Deletes are represented as tombstones, not immediate physical removal.
- A tombstone in the MemTable or a newer SSTable hides older values in older SSTables.

## API Contract

### `PUT /kv/{key}`

- Body: raw bytes (`application/octet-stream` is recommended).
- Behavior: create if missing, overwrite if present.
- Status codes:
1. `201 Created` when key is new.
2. `200 OK` when key existed and was overwritten.
3. `400 Bad Request` for invalid key/body.
4. `413 Payload Too Large` when body exceeds configured limit.
5. `500 Internal Server Error` if the WAL append fails.

### `GET /kv/{key}`

- Returns latest stored value.
- Success content type: `application/octet-stream`.
- Status codes:
1. `200 OK` if found.
2. `404 Not Found` if key is missing.

### `DELETE /kv/{key}`

- Deletes key if present.
- Status codes:
1. `204 No Content` if deleted.
2. `404 Not Found` if key is missing.
3. `500 Internal Server Error` if the WAL append fails.

### `GET /health`

- Returns `200 OK` with:
```json
{"status":"ok"}
```

## Semantics

- Keys are unique.
- `PUT` overwrites previous value for the same key.
- `GET` always returns the latest written value.
- `DELETE` writes a tombstone that makes the key logically absent.
- On startup, the server loads SSTables from disk and then replays the WAL into the active MemTable.
- If replay encounters a truncated final record or a CRC mismatch at the tail, recovery stops at the last valid record.
- Reads check the MemTable first, then SSTables newest-first, so the latest version always wins.
- Idempotency:
1. Repeating `PUT` with the same key/value keeps final state unchanged.
2. Repeating `DELETE` is safe and predictable (`204` then `404` once the key is already gone).

## Validation Rules

- Key must be non-empty.
- Key is treated as one path segment (`/kv/{key}`), so embedded `/` is invalid.
- Value/body size is limited (default: `1 MiB`).

## Error Format

Errors are returned as stable JSON:

```json
{"error":"..."}
```

Examples:
- `{"error":"invalid key"}`
- `{"error":"invalid body"}`
- `{"error":"key not found"}`
- `{"error":"request body too large"}`
- `{"error":"internal server error"}`

## Project Structure

```text
.
├── main.go
├── server/
│   ├── server.go
│   └── server_test.go
└── store/
    ├── store.go
    └── store_test.go
```

Current store files also include `store/wal.go` and `store/wal_codec.go`.
Phase 3 also adds `store/sstable.go` for SSTable writing, loading, indexing, and point lookups.

## Requirements

- Go 1.22+.

## How To Run

From the project root:

```powershell
go run .
```

Defaults:

- `PORT=8080`
- `WAL_PATH=data/kv.wal`
- `DATA_DIR=data`
- `WAL_SYNC_MODE=always`
- `MEMTABLE_FLUSH_BYTES=4194304`

Custom example:

```powershell
$env:PORT="9090"
$env:WAL_PATH="data/kv.wal"
$env:DATA_DIR="data"
$env:WAL_SYNC_MODE="always"
$env:MEMTABLE_FLUSH_BYTES="4194304"
go run .
```

Supported `WAL_SYNC_MODE` values:

- `always`
- `never`

## How To Test

Run all tests:

```powershell
go test ./...
```

Run race detector:

```powershell
go test -race ./...
```

## Quick Manual Checks

Create:

```powershell
curl -X PUT "http://localhost:8080/kv/name" --data-binary "alice" -i
```

Read:

```powershell
curl "http://localhost:8080/kv/name" -i
```

Delete:

```powershell
curl -X DELETE "http://localhost:8080/kv/name" -i
```

Persistence check:

1. `curl -X PUT "http://localhost:8080/kv/name" --data-binary "alice" -i`
2. Stop the server.
3. Start the server again with the same `WAL_PATH`.
4. `curl "http://localhost:8080/kv/name" -i`

Phase 3 flush demo:

1. Start the server with a small flush threshold, for example `MEMTABLE_FLUSH_BYTES=32`.
2. Write several keys with `PUT /kv/{key}`.
3. Check the data directory and observe `sst_*.dat` files appearing.
4. Delete one of the flushed keys.
5. Write more keys until another flush happens.
6. `GET` on the deleted key should still return `404` because the newer tombstone masks the stale value in the older SSTable.
