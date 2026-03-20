package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPutThenGet(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)

	created, err := s.Put("a", []byte("value-1"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if !created {
		t.Fatalf("Put() created = false, want true")
	}

	got, found := s.Get("a")
	if !found {
		t.Fatalf("Get() found = false, want true")
	}
	if string(got) != "value-1" {
		t.Fatalf("Get() value = %q, want %q", string(got), "value-1")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)

	created, err := s.Put("a", []byte("value-1"))
	if err != nil {
		t.Fatalf("first Put() error = %v", err)
	}
	if !created {
		t.Fatalf("first Put() created = false, want true")
	}
	created, err = s.Put("a", []byte("value-2"))
	if err != nil {
		t.Fatalf("second Put() error = %v", err)
	}
	if created {
		t.Fatalf("second Put() created = true, want false")
	}

	got, found := s.Get("a")
	if !found {
		t.Fatalf("Get() found = false, want true")
	}
	if string(got) != "value-2" {
		t.Fatalf("Get() value = %q, want %q", string(got), "value-2")
	}
}

func TestDeleteRemoves(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)
	if _, err := s.Put("a", []byte("value")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	deleted, err := s.Delete("a")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !deleted {
		t.Fatalf("Delete() = false, want true")
	}

	_, found := s.Get("a")
	if found {
		t.Fatalf("Get() found = true after delete, want false")
	}
}

func TestGetMissingReturnsNotFound(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)

	_, found := s.Get("missing")
	if found {
		t.Fatalf("Get() found = true, want false")
	}
}

func TestPutValidation(t *testing.T) {
	s := NewInMemoryStore(4)

	if _, err := s.Put("", []byte("x")); err != ErrEmptyKey {
		t.Fatalf("Put(empty key) error = %v, want %v", err, ErrEmptyKey)
	}

	if _, err := s.Put("k", []byte("12345")); err != ErrValueTooLarge {
		t.Fatalf("Put(oversize) error = %v, want %v", err, ErrValueTooLarge)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)
	const workers = 32
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				v := []byte(fmt.Sprintf("worker-%d-%d", id, n))
				if _, err := s.Put("shared", v); err != nil {
					t.Errorf("Put() error: %v", err)
					return
				}
				if _, found := s.Get("shared"); !found {
					t.Errorf("Get() found = false, want true")
					return
				}
			}
		}(i)
	}

	wg.Wait()

	if _, found := s.Get("shared"); !found {
		t.Fatalf("final Get() found = false, want true")
	}
}

func TestDeleteMissingReturnsFalse(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)

	deleted, err := s.Delete("missing")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted {
		t.Fatalf("Delete() = true, want false")
	}
}

func TestStoreRecoveryFromWAL(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "kv.wal")

	s, err := NewStore(Config{
		MaxValueSize: DefaultMaxValueSize,
		WALPath:      walPath,
		SyncMode:     SyncAlways,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if _, err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put(alpha) error = %v", err)
	}
	if _, err := s.Put("beta", []byte("two")); err != nil {
		t.Fatalf("Put(beta) error = %v", err)
	}
	if deleted, err := s.Delete("alpha"); err != nil || !deleted {
		t.Fatalf("Delete(alpha) = (%v, %v), want (true, nil)", deleted, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	recovered, err := NewStore(Config{
		MaxValueSize: DefaultMaxValueSize,
		WALPath:      walPath,
		SyncMode:     SyncAlways,
	})
	if err != nil {
		t.Fatalf("NewStore(recover) error = %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("Close(recovered) error = %v", err)
		}
	}()

	if _, found := recovered.Get("alpha"); found {
		t.Fatalf("Get(alpha) found = true, want false")
	}
	got, found := recovered.Get("beta")
	if !found {
		t.Fatalf("Get(beta) found = false, want true")
	}
	if string(got) != "two" {
		t.Fatalf("Get(beta) value = %q, want %q", string(got), "two")
	}
}

func TestStoreRecoveryIgnoresTruncatedTail(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "kv.wal")

	s, err := NewStore(Config{
		MaxValueSize: DefaultMaxValueSize,
		WALPath:      walPath,
		SyncMode:     SyncNever,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if _, err := s.Put("persisted", []byte("ok")); err != nil {
		t.Fatalf("Put(persisted) error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	record, err := EncodePut("broken", []byte("tail"))
	if err != nil {
		t.Fatalf("EncodePut() error = %v", err)
	}
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.Write(record[:len(record)-3]); err != nil {
		_ = file.Close()
		t.Fatalf("Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close(file) error = %v", err)
	}

	recovered, err := NewStore(Config{
		MaxValueSize: DefaultMaxValueSize,
		WALPath:      walPath,
		SyncMode:     SyncNever,
	})
	if err != nil {
		t.Fatalf("NewStore(recover) error = %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("Close(recovered) error = %v", err)
		}
	}()

	if _, found := recovered.Get("broken"); found {
		t.Fatalf("Get(broken) found = true, want false")
	}
	got, found := recovered.Get("persisted")
	if !found {
		t.Fatalf("Get(persisted) found = false, want true")
	}
	if string(got) != "ok" {
		t.Fatalf("Get(persisted) value = %q, want %q", string(got), "ok")
	}
}

func TestStorePutFailsWhenWALPathIsDirectory(t *testing.T) {
	tempDir := t.TempDir()

	_, err := NewStore(Config{
		MaxValueSize: DefaultMaxValueSize,
		WALPath:      tempDir,
		SyncMode:     SyncAlways,
	})
	if err == nil {
		t.Fatalf("NewStore() error = nil, want non-nil")
	}
}

func TestDecodeRecordDetectsCRCMismatch(t *testing.T) {
	record, err := EncodePut("key", []byte("value"))
	if err != nil {
		t.Fatalf("EncodePut() error = %v", err)
	}

	record[len(record)-1] ^= 0xFF
	_, err = DecodeRecord(bytes.NewReader(record))
	if !errors.Is(err, ErrCRCMismatch) {
		t.Fatalf("DecodeRecord() error = %v, want %v", err, ErrCRCMismatch)
	}
}
