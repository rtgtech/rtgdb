package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableWriteAndLookup(t *testing.T) {
	dir := t.TempDir()

	table, err := WriteSSTable(dir, 7, []SSTableEntry{
		{Key: "alpha", Entry: entry{Value: []byte("one")}},
		{Key: "beta", Entry: entry{Deleted: true}},
		{Key: "gamma", Entry: entry{Value: []byte("three")}},
	})
	if err != nil {
		t.Fatalf("WriteSSTable() error = %v", err)
	}
	defer func() {
		if err := table.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if table.minKey != "alpha" {
		t.Fatalf("minKey = %q, want %q", table.minKey, "alpha")
	}
	if table.maxKey != "gamma" {
		t.Fatalf("maxKey = %q, want %q", table.maxKey, "gamma")
	}

	got, found, err := table.Get("alpha")
	if err != nil {
		t.Fatalf("Get(alpha) error = %v", err)
	}
	if !found {
		t.Fatalf("Get(alpha) found = false, want true")
	}
	if got.Deleted {
		t.Fatalf("Get(alpha) Deleted = true, want false")
	}
	if string(got.Value) != "one" {
		t.Fatalf("Get(alpha) value = %q, want %q", string(got.Value), "one")
	}

	got, found, err = table.Get("beta")
	if err != nil {
		t.Fatalf("Get(beta) error = %v", err)
	}
	if !found {
		t.Fatalf("Get(beta) found = false, want true")
	}
	if !got.Deleted {
		t.Fatalf("Get(beta) Deleted = false, want true")
	}

	_, found, err = table.Get("missing")
	if err != nil {
		t.Fatalf("Get(missing) error = %v", err)
	}
	if found {
		t.Fatalf("Get(missing) found = true, want false")
	}
}

func TestFlushCreatesSSTableAndClearsMemTable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{
		MaxValueSize:           DefaultMaxValueSize,
		WALPath:                filepath.Join(dir, "kv.wal"),
		SyncMode:               SyncNever,
		DataDir:                dir,
		MemTableFlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if _, err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	s.mu.RLock()
	memCount := len(s.memtable)
	tableCount := len(s.sstables)
	s.mu.RUnlock()

	if memCount != 0 {
		t.Fatalf("len(memtable) = %d, want 0", memCount)
	}
	if tableCount != 1 {
		t.Fatalf("len(sstables) = %d, want 1", tableCount)
	}

	if _, err := os.Stat(filepath.Join(dir, formatSSTableFileName(0))); err != nil {
		t.Fatalf("Stat(sstable) error = %v", err)
	}

	got, found := s.Get("alpha")
	if !found {
		t.Fatalf("Get() found = false, want true")
	}
	if string(got) != "one" {
		t.Fatalf("Get() value = %q, want %q", string(got), "one")
	}
}

func TestMemTableOverridesSSTable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{
		MaxValueSize:           DefaultMaxValueSize,
		WALPath:                filepath.Join(dir, "kv.wal"),
		SyncMode:               SyncNever,
		DataDir:                dir,
		MemTableFlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if _, err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put(alpha=one) error = %v", err)
	}

	s.mu.Lock()
	s.memTableFlushThreshold = DefaultMemTableFlushThreshold
	s.mu.Unlock()

	if _, err := s.Put("alpha", []byte("two")); err != nil {
		t.Fatalf("Put(alpha=two) error = %v", err)
	}

	got, found := s.Get("alpha")
	if !found {
		t.Fatalf("Get(alpha) found = false, want true")
	}
	if string(got) != "two" {
		t.Fatalf("Get(alpha) value = %q, want %q", string(got), "two")
	}
}

func TestNewerSSTableOverridesOlderSSTable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{
		MaxValueSize:           DefaultMaxValueSize,
		WALPath:                filepath.Join(dir, "kv.wal"),
		SyncMode:               SyncNever,
		DataDir:                dir,
		MemTableFlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if _, err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put(alpha=one) error = %v", err)
	}
	if _, err := s.Put("alpha", []byte("two")); err != nil {
		t.Fatalf("Put(alpha=two) error = %v", err)
	}

	got, found := s.Get("alpha")
	if !found {
		t.Fatalf("Get(alpha) found = false, want true")
	}
	if string(got) != "two" {
		t.Fatalf("Get(alpha) value = %q, want %q", string(got), "two")
	}
}

func TestMemTableTombstoneOverridesSSTable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{
		MaxValueSize:           DefaultMaxValueSize,
		WALPath:                filepath.Join(dir, "kv.wal"),
		SyncMode:               SyncNever,
		DataDir:                dir,
		MemTableFlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if _, err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put(alpha=one) error = %v", err)
	}

	s.mu.Lock()
	s.memTableFlushThreshold = DefaultMemTableFlushThreshold
	s.mu.Unlock()

	deleted, err := s.Delete("alpha")
	if err != nil {
		t.Fatalf("Delete(alpha) error = %v", err)
	}
	if !deleted {
		t.Fatalf("Delete(alpha) = false, want true")
	}

	if _, found := s.Get("alpha"); found {
		t.Fatalf("Get(alpha) found = true, want false")
	}
}

func TestNewerSSTableTombstoneOverridesOlderValue(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{
		MaxValueSize:           DefaultMaxValueSize,
		WALPath:                filepath.Join(dir, "kv.wal"),
		SyncMode:               SyncNever,
		DataDir:                dir,
		MemTableFlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if _, err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put(alpha=one) error = %v", err)
	}
	if deleted, err := s.Delete("alpha"); err != nil || !deleted {
		t.Fatalf("Delete(alpha) = (%v, %v), want (true, nil)", deleted, err)
	}

	if _, found := s.Get("alpha"); found {
		t.Fatalf("Get(alpha) found = true, want false")
	}
}

func TestRestartLoadsSSTablesAndWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		MaxValueSize:           DefaultMaxValueSize,
		WALPath:                filepath.Join(dir, "kv.wal"),
		SyncMode:               SyncAlways,
		DataDir:                dir,
		MemTableFlushThreshold: 1,
	}

	s, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if _, err := s.Put("flushed", []byte("disk")); err != nil {
		t.Fatalf("Put(flushed) error = %v", err)
	}
	if _, err := s.Put("kept", []byte("disk-too")); err != nil {
		t.Fatalf("Put(kept) error = %v", err)
	}

	s.mu.Lock()
	s.memTableFlushThreshold = DefaultMemTableFlushThreshold
	s.mu.Unlock()

	if _, err := s.Put("wal-only", []byte("mem")); err != nil {
		t.Fatalf("Put(wal-only) error = %v", err)
	}
	if deleted, err := s.Delete("flushed"); err != nil || !deleted {
		t.Fatalf("Delete(flushed) = (%v, %v), want (true, nil)", deleted, err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	recovered, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore(recover) error = %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("Close(recovered) error = %v", err)
		}
	}()

	if _, found := recovered.Get("flushed"); found {
		t.Fatalf("Get(flushed) found = true, want false")
	}
	got, found := recovered.Get("kept")
	if !found {
		t.Fatalf("Get(kept) found = false, want true")
	}
	if string(got) != "disk-too" {
		t.Fatalf("Get(kept) value = %q, want %q", string(got), "disk-too")
	}
	got, found = recovered.Get("wal-only")
	if !found {
		t.Fatalf("Get(wal-only) found = false, want true")
	}
	if string(got) != "mem" {
		t.Fatalf("Get(wal-only) value = %q, want %q", string(got), "mem")
	}
}
