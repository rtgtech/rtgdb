package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"rtgdb/store"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := store.NewInMemoryStore(DefaultMaxBodyBytes)
	return httptest.NewServer(NewHandler(s, DefaultMaxBodyBytes))
}

func TestHealth(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/json")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "{\"status\":\"ok\"}\n" {
		t.Fatalf("body = %q, want %q", string(body), "{\"status\":\"ok\"}\n")
	}
}

func TestPutGetDeleteFlow(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	putURL := ts.URL + "/kv/mykey"
	getURL := ts.URL + "/kv/mykey"

	req, _ := http.NewRequest(http.MethodPut, putURL, bytes.NewBufferString("value-1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("first PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first PUT status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	req, _ = http.NewRequest(http.MethodPut, putURL, bytes.NewBufferString("value-2"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("second PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second PUT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	resp, err = http.Get(getURL)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("GET Content-Type = %q, want %q", got, "application/octet-stream")
	}
	if string(body) != "value-2" {
		t.Fatalf("GET body = %q, want %q", string(body), "value-2")
	}

	req, _ = http.NewRequest(http.MethodDelete, putURL, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	resp, err = http.Get(getURL)
	if err != nil {
		t.Fatalf("GET after delete error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestDeleteMissingReturnsNotFound(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/kv/missing", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE missing error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestInvalidKeyReturnsBadRequest(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/", bytes.NewBufferString("x"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT invalid key error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPayloadTooLarge(t *testing.T) {
	s := store.NewInMemoryStore(4)
	ts := httptest.NewServer(NewHandler(s, 4))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", bytes.NewBufferString("12345"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT oversize error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestConcurrencySanity(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	const workers = 30
	const iterations = 80
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				body := []byte("w")
				req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/shared", bytes.NewReader(body))
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Errorf("PUT error: %v", err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
					t.Errorf("PUT status = %d, want 201 or 200", resp.StatusCode)
					return
				}

				getResp, err := http.Get(ts.URL + "/kv/shared")
				if err != nil {
					t.Errorf("GET error: %v", err)
					return
				}
				getResp.Body.Close()
				if getResp.StatusCode != http.StatusOK {
					t.Errorf("GET status = %d, want %d", getResp.StatusCode, http.StatusOK)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}
