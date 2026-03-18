package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestPutThenGet(t *testing.T) {
	s := NewInMemoryStore(DefaultMaxValueSize)

	if err := s.Put("a", []byte("value-1")); err != nil {
		t.Fatalf("Put() error = %v", err)
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

	if err := s.Put("a", []byte("value-1")); err != nil {
		t.Fatalf("first Put() error = %v", err)
	}
	if err := s.Put("a", []byte("value-2")); err != nil {
		t.Fatalf("second Put() error = %v", err)
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
	if err := s.Put("a", []byte("value")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	deleted := s.Delete("a")
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

	if err := s.Put("", []byte("x")); err != ErrEmptyKey {
		t.Fatalf("Put(empty key) error = %v, want %v", err, ErrEmptyKey)
	}

	if err := s.Put("k", []byte("12345")); err != ErrValueTooLarge {
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
				if err := s.Put("shared", v); err != nil {
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
