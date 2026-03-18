package store

import (
	"errors"
	"sync"
)

const (
	// DefaultMaxValueSize sets a conservative upper bound for values.
	DefaultMaxValueSize = 1 << 20 // 1 MiB
)

var (
	ErrEmptyKey      = errors.New("invalid key")
	ErrValueTooLarge = errors.New("value too large")
)

// Store defines the key-value operations.
type Store interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, bool)
	Delete(key string) bool
}

// InMemoryStore is a map-backed, concurrency-safe store.
type InMemoryStore struct {
	mu           sync.RWMutex
	data         map[string][]byte
	maxValueSize int
}

func NewInMemoryStore(maxValueSize int) *InMemoryStore {
	if maxValueSize <= 0 {
		maxValueSize = DefaultMaxValueSize
	}
	return &InMemoryStore{
		data:         make(map[string][]byte),
		maxValueSize: maxValueSize,
	}
}

// ValidateKey ensures key semantics are stable across layers.
func ValidateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	return nil
}

func (s *InMemoryStore) Put(key string, value []byte) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	if len(value) > s.maxValueSize {
		return ErrValueTooLarge
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)

	s.mu.Lock()
	s.data[key] = cloned
	s.mu.Unlock()

	return nil
}

func (s *InMemoryStore) Get(key string) ([]byte, bool) {
	if ValidateKey(key) != nil {
		return nil, false
	}

	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned, true
}

func (s *InMemoryStore) Delete(key string) bool {
	if ValidateKey(key) != nil {
		return false
	}

	s.mu.Lock()
	_, found := s.data[key]
	if found {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return found
}
