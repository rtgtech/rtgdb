# In-Memory Key-Value Store (Go)

Simple HTTP key-value service with a concurrency-safe in-memory store.

## Features

- `PUT /kv/{key}` creates or overwrites a value.
- `GET /kv/{key}` returns the latest value bytes.
- `DELETE /kv/{key}` removes a key.
- `GET /health` returns a health payload.
- Thread-safe map store using `sync.RWMutex`.
- Unit tests + HTTP integration tests + concurrency sanity tests.

## API Contract

### `PUT /kv/{key}`

- Body: raw bytes (`application/octet-stream` is recommended).
- Behavior: create if missing, overwrite if present.
- Status codes:
1. `201 Created` when key is new.
2. `200 OK` when key existed and was overwritten.
3. `400 Bad Request` for invalid key/body.
4. `413 Payload Too Large` when body exceeds configured limit.

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

### `GET /health`

- Returns `200 OK` with:
```json
{"status":"ok"}
```

## Semantics

- Keys are unique.
- `PUT` overwrites previous value for the same key.
- `GET` always returns the latest written value.
- `DELETE` removes the key entirely.
- Idempotency:
1. Repeating `PUT` with same key/value keeps final state unchanged.
2. Repeating `DELETE` is safe and predictable (`204` then `404` for subsequent missing key).

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

## Requirements

- Go 1.22+ (tested in this workspace with Go 1.25 toolchain).

## How To Run

From the project root:

```powershell
go run .
```

Server starts on port `8080` by default.

Set a custom port:

```powershell
$env:PORT="9090"
go run .
```

## How To Test

Run all tests:

```powershell
go test ./...
```

Run race detector (recommended):

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
