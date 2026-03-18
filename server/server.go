package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"rtgdb/store"
)

const (
	DefaultMaxBodyBytes = 1 << 20 // 1 MiB
)

type API struct {
	store        store.Store
	maxBodyBytes int
}

func NewHandler(s store.Store, maxBodyBytes int) http.Handler {
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}
	return &API{
		store:        s,
		maxBodyBytes: maxBodyBytes,
	}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	if !strings.HasPrefix(r.URL.Path, "/kv/") {
		writeJSONError(w, http.StatusNotFound, "not found")
		return
	}

	key, err := parseKey(r.URL.Path)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid key")
		return
	}

	switch r.Method {
	case http.MethodPut:
		a.handlePut(w, r, key)
	case http.MethodGet:
		a.handleGet(w, key)
	case http.MethodDelete:
		a.handleDelete(w, key)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *API) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	_, existed := a.store.Get(key)

	value, err := readBodyWithLimit(r.Body, a.maxBodyBytes)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid body")
		return
	}

	err = a.store.Put(key, value)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrEmptyKey):
			writeJSONError(w, http.StatusBadRequest, "invalid key")
		case errors.Is(err, store.ErrValueTooLarge):
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		default:
			writeJSONError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	if existed {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleGet(w http.ResponseWriter, key string) {
	value, found := a.store.Get(key)
	if !found {
		writeJSONError(w, http.StatusNotFound, "key not found")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (a *API) handleDelete(w http.ResponseWriter, key string) {
	deleted := a.store.Delete(key)
	if !deleted {
		writeJSONError(w, http.StatusNotFound, "key not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var errBodyTooLarge = errors.New("request body too large")

func readBodyWithLimit(body io.ReadCloser, limit int) ([]byte, error) {
	defer body.Close()
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}

	limited := io.LimitReader(body, int64(limit+1))
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(payload) > limit {
		return nil, errBodyTooLarge
	}
	return payload, nil
}

func parseKey(path string) (string, error) {
	key := strings.TrimPrefix(path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		return "", store.ErrEmptyKey
	}
	return key, nil
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
