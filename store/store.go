package store

import (
	"errors"
	"path/filepath"
	"sync"
)

const (
	// DefaultMaxValueSize sets a conservative upper bound for values.
	DefaultMaxValueSize = 1 << 20 // 1 MiB
	// DefaultMemTableFlushThreshold is the approximate number of bytes held in memory
	// before the store flushes the active MemTable into a new SSTable.
	DefaultMemTableFlushThreshold = 4 << 20 // 4 MiB
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
	MaxValueSize           int
	WALPath                string
	SyncMode               SyncMode
	DataDir                string
	MemTableFlushThreshold int
}

type entry struct {
	Value   []byte
	Deleted bool
}

// InMemoryStore is an LSM-style store with a mutable MemTable and immutable SSTables.
type InMemoryStore struct {
	mu                     sync.RWMutex
	memtable               map[string]entry
	memTableSize           int
	maxValueSize           int
	memTableFlushThreshold int
	dataDir                string
	nextSSTableID          uint64
	wal                    *WAL
	sstables               []*SSTable
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

	dataDir := cfg.DataDir
	if dataDir == "" && cfg.WALPath != "" {
		dataDir = filepath.Dir(cfg.WALPath)
	}

	flushThreshold := cfg.MemTableFlushThreshold
	if flushThreshold <= 0 && dataDir != "" {
		flushThreshold = DefaultMemTableFlushThreshold
	}

	s := &InMemoryStore{
		memtable:               make(map[string]entry),
		maxValueSize:           maxValueSize,
		memTableFlushThreshold: flushThreshold,
		dataDir:                dataDir,
	}

	if dataDir != "" {
		tables, nextID, err := LoadSSTables(dataDir)
		if err != nil {
			return nil, err
		}
		s.sstables = tables
		s.nextSSTableID = nextID
	}

	if cfg.WALPath == "" {
		return s, nil
	}

	wal, err := OpenWAL(cfg.WALPath, cfg.SyncMode)
	if err != nil {
		s.closeSSTables()
		return nil, err
	}

	if err := wal.Replay(func(record Record) error {
		switch record.Op {
		case OpPut:
			s.setMemTableEntry(record.Key, entry{Value: cloneBytes(record.Value)})
		case OpDelete:
			s.setMemTableEntry(record.Key, entry{Deleted: true})
		}
		return nil
	}); err != nil {
		_ = wal.Close()
		s.closeSSTables()
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

	current, found, err := s.lookupEntryLocked(key)
	if err != nil {
		return false, err
	}
	created := !found || current.Deleted

	if s.wal != nil {
		if err := s.wal.Append(record); err != nil {
			return false, err
		}
	}

	s.setMemTableEntry(key, entry{Value: cloned})
	_ = s.maybeFlushLocked()
	return created, nil
}

func (s *InMemoryStore) Get(key string) ([]byte, bool) {
	if ValidateKey(key) != nil {
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	foundEntry, found, err := s.lookupEntryLocked(key)
	if err != nil || !found || foundEntry.Deleted {
		return nil, false
	}
	return cloneBytes(foundEntry.Value), true
}

func (s *InMemoryStore) Delete(key string) (bool, error) {
	if err := ValidateKey(key); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, found, err := s.lookupEntryLocked(key)
	if err != nil {
		return false, err
	}
	if !found || current.Deleted {
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

	s.setMemTableEntry(key, entry{Deleted: true})
	_ = s.maybeFlushLocked()
	return true, nil
}

func (s *InMemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var closeErr error
	if s.wal != nil {
		closeErr = s.wal.Close()
		s.wal = nil
	}
	s.closeSSTables()
	return closeErr
}

func (s *InMemoryStore) lookupEntryLocked(key string) (entry, bool, error) {
	if foundEntry, ok := s.memtable[key]; ok {
		return cloneEntry(foundEntry), true, nil
	}

	for i := len(s.sstables) - 1; i >= 0; i-- {
		table := s.sstables[i]
		if key < table.minKey || key > table.maxKey {
			continue
		}

		foundEntry, found, err := table.Get(key)
		if err != nil {
			return entry{}, false, err
		}
		if found {
			return foundEntry, true, nil
		}
	}

	return entry{}, false, nil
}

func (s *InMemoryStore) maybeFlushLocked() error {
	if s.dataDir == "" || s.memTableFlushThreshold <= 0 {
		return nil
	}
	if s.memTableSize < s.memTableFlushThreshold || len(s.memtable) == 0 {
		return nil
	}

	table, err := WriteSSTable(s.dataDir, s.nextSSTableID, s.sortedMemTableEntriesLocked())
	if err != nil {
		return err
	}

	s.nextSSTableID++
	s.sstables = append(s.sstables, table)
	s.memtable = make(map[string]entry)
	s.memTableSize = 0

	if s.wal != nil {
		_ = s.wal.Reset()
	}

	return nil
}

func (s *InMemoryStore) sortedMemTableEntriesLocked() []SSTableEntry {
	entries := make([]SSTableEntry, 0, len(s.memtable))
	for key, value := range s.memtable {
		entries = append(entries, SSTableEntry{
			Key:   key,
			Entry: cloneEntry(value),
		})
	}
	sortSSTableEntries(entries)
	return entries
}

func (s *InMemoryStore) setMemTableEntry(key string, value entry) {
	if existing, ok := s.memtable[key]; ok {
		s.memTableSize -= memTableEntrySize(key, existing)
	}
	cloned := cloneEntry(value)
	s.memtable[key] = cloned
	s.memTableSize += memTableEntrySize(key, cloned)
}

func (s *InMemoryStore) closeSSTables() {
	for _, table := range s.sstables {
		_ = table.Close()
	}
	s.sstables = nil
}

func memTableEntrySize(key string, value entry) int {
	return len(key) + len(value.Value) + 1
}

func cloneEntry(value entry) entry {
	return entry{
		Value:   cloneBytes(value.Value),
		Deleted: value.Deleted,
	}
}

func cloneBytes(value []byte) []byte {
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
