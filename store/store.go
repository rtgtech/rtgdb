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
	Put(key string, value []byte) (bool, error)
	Get(key string) ([]byte, bool)
	Delete(key string) (bool, error)
	Close() error
}

type Config struct {
	MaxValueSize int
	WALPath      string
	SyncMode     SyncMode
}

// InMemoryStore is a map-backed, concurrency-safe store.
type InMemoryStore struct {
	mu           sync.RWMutex
	data         map[string][]byte
	maxValueSize int
	wal          *WAL
}

func NewInMemoryStore(maxValueSize int) *InMemoryStore {
	cfg := Config{MaxValueSize: maxValueSize}
	s, _ := NewStore(cfg)
	return s
}

func NewStore(cfg Config) (*InMemoryStore, error) {
	maxValueSize := cfg.MaxValueSize
	if maxValueSize <= 0 {
		maxValueSize = DefaultMaxValueSize
	}

	s := &InMemoryStore{
		data:         make(map[string][]byte),
		maxValueSize: maxValueSize,
	}

	if cfg.WALPath == "" {
		return s, nil
	}

	wal, err := OpenWAL(cfg.WALPath, cfg.SyncMode)
	if err != nil {
		return nil, err
	}

	if err := wal.Replay(func(record Record) error {
		switch record.Op {
		case OpPut:
			s.data[record.Key] = cloneBytes(record.Value)
		case OpDelete:
			delete(s.data, record.Key)
		}
		return nil
	}); err != nil {
		_ = wal.Close()
		return nil, err
	}

	s.wal = wal
	return s, nil
}

// ValidateKey ensures key semantics are stable across layers.
func ValidateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	return nil
}

func (s *InMemoryStore) Put(key string, value []byte) (bool, error) {
	if err := ValidateKey(key); err != nil {
		return false, err
	}
	if len(value) > s.maxValueSize {
		return false, ErrValueTooLarge
	}

	cloned := cloneBytes(value)
	record, err := EncodePut(key, cloned)
	if err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, existed := s.data[key]
	if s.wal != nil {
		if err := s.wal.Append(record); err != nil {
			return false, err
		}
	}
	s.data[key] = cloned

	return !existed, nil
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

func (s *InMemoryStore) Delete(key string) (bool, error) {
	if err := ValidateKey(key); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, found := s.data[key]
	if !found {
		return false, nil
	}

	if s.wal != nil {
		record, err := EncodeDelete(key)
		if err != nil {
			return false, err
		}
		if err := s.wal.Append(record); err != nil {
			return false, err
		}
	}

	delete(s.data, key)
	return true, nil
}

func (s *InMemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	return err
}

func cloneBytes(value []byte) []byte {
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
