package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	sstableDataPattern = "sst_*.dat"
	sstableFilePrefix  = "sst_"
	sstableFileSuffix  = ".dat"
	sstableTempSuffix  = ".tmp"
)

var ErrSSTableRecordTooLarge = errors.New("sstable record too large")

type SSTableEntry struct {
	Key   string
	Entry entry
}

type SSTable struct {
	id     uint64
	path   string
	file   *os.File
	index  map[string]int64
	minKey string
	maxKey string
}

func LoadSSTables(dir string) ([]*SSTable, uint64, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, err
	}

	paths, err := filepath.Glob(filepath.Join(dir, sstableDataPattern))
	if err != nil {
		return nil, 0, err
	}

	sort.Slice(paths, func(i, j int) bool {
		return extractSSTableID(paths[i]) < extractSSTableID(paths[j])
	})

	tables := make([]*SSTable, 0, len(paths))
	var nextID uint64
	for _, path := range paths {
		table, err := openSSTable(path)
		if err != nil {
			for _, existing := range tables {
				_ = existing.Close()
			}
			return nil, 0, err
		}
		tables = append(tables, table)
		if table.id >= nextID {
			nextID = table.id + 1
		}
	}

	return tables, nextID, nil
}

func WriteSSTable(dir string, id uint64, entries []SSTableEntry) (*SSTable, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	tmpPath := filepath.Join(dir, formatSSTableFileName(id)+sstableTempSuffix)
	finalPath := filepath.Join(dir, formatSSTableFileName(id))

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	index := make(map[string]int64, len(entries))
	var offset int64
	for _, item := range entries {
		record, err := encodeSSTableRecord(item.Key, item.Entry)
		if err != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
			return nil, err
		}

		index[item.Key] = offset
		n, err := file.Write(record)
		if err != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
			return nil, err
		}
		offset += int64(n)
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}

	reader, err := os.Open(finalPath)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		id:     id,
		path:   finalPath,
		file:   reader,
		index:  index,
		minKey: entries[0].Key,
		maxKey: entries[len(entries)-1].Key,
	}, nil
}

func (t *SSTable) Get(key string) (entry, bool, error) {
	offset, ok := t.index[key]
	if !ok {
		return entry{}, false, nil
	}

	record, _, err := readSSTableRecordAt(t.file, offset)
	if err != nil {
		return entry{}, false, err
	}
	return record.Entry, true, nil
}

func (t *SSTable) Close() error {
	if t == nil || t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}

func openSSTable(path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	index := make(map[string]int64)
	var minKey string
	var maxKey string
	var offset int64
	for {
		record, nextOffset, err := readSSTableRecordAt(file, offset)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = file.Close()
			return nil, err
		}

		index[record.Key] = offset
		if minKey == "" {
			minKey = record.Key
		}
		maxKey = record.Key
		offset = nextOffset
	}

	return &SSTable{
		id:     extractSSTableID(path),
		path:   path,
		file:   file,
		index:  index,
		minKey: minKey,
		maxKey: maxKey,
	}, nil
}

func encodeSSTableRecord(key string, value entry) ([]byte, error) {
	if err := ValidateKey(key); err != nil {
		return nil, err
	}

	keyBytes := []byte(key)
	if len(keyBytes) > int(^uint32(0)) || len(value.Value) > int(^uint32(0)) {
		return nil, ErrSSTableRecordTooLarge
	}

	recordLen := 4 + len(keyBytes) + 1 + 4 + len(value.Value)
	if recordLen > int(^uint32(0)) {
		return nil, ErrSSTableRecordTooLarge
	}

	buf := make([]byte, recordLen)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(keyBytes)))
	copy(buf[4:4+len(keyBytes)], keyBytes)

	deletedOffset := 4 + len(keyBytes)
	if value.Deleted {
		buf[deletedOffset] = 1
	}

	valueLenOffset := deletedOffset + 1
	binary.BigEndian.PutUint32(buf[valueLenOffset:valueLenOffset+4], uint32(len(value.Value)))
	copy(buf[valueLenOffset+4:], value.Value)
	return buf, nil
}

func readSSTableRecordAt(file *os.File, offset int64) (SSTableEntry, int64, error) {
	var keyLenBuf [4]byte
	n, err := file.ReadAt(keyLenBuf[:], offset)
	if errors.Is(err, io.EOF) && n == 0 {
		return SSTableEntry{}, offset, io.EOF
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return SSTableEntry{}, offset, err
	}
	if n != len(keyLenBuf) {
		return SSTableEntry{}, offset, io.ErrUnexpectedEOF
	}

	keyLen := int(binary.BigEndian.Uint32(keyLenBuf[:]))
	keyBytes := make([]byte, keyLen)
	if _, err := file.ReadAt(keyBytes, offset+4); err != nil {
		return SSTableEntry{}, offset, err
	}

	var deletedBuf [1]byte
	if _, err := file.ReadAt(deletedBuf[:], offset+4+int64(keyLen)); err != nil {
		return SSTableEntry{}, offset, err
	}

	var valueLenBuf [4]byte
	valueLenOffset := offset + 4 + int64(keyLen) + 1
	if _, err := file.ReadAt(valueLenBuf[:], valueLenOffset); err != nil {
		return SSTableEntry{}, offset, err
	}
	valueLen := int(binary.BigEndian.Uint32(valueLenBuf[:]))

	valueOffset := valueLenOffset + 4
	valueBytes := make([]byte, valueLen)
	if _, err := file.ReadAt(valueBytes, valueOffset); err != nil {
		return SSTableEntry{}, offset, err
	}

	record := SSTableEntry{
		Key: string(keyBytes),
		Entry: entry{
			Value:   cloneBytes(valueBytes),
			Deleted: deletedBuf[0] == 1,
		},
	}
	nextOffset := valueOffset + int64(valueLen)
	return record, nextOffset, nil
}

func sortSSTableEntries(entries []SSTableEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
}

func formatSSTableFileName(id uint64) string {
	return fmt.Sprintf("%s%06d%s", sstableFilePrefix, id, sstableFileSuffix)
}

func extractSSTableID(path string) uint64 {
	name := filepath.Base(path)
	idPart := strings.TrimSuffix(strings.TrimPrefix(name, sstableFilePrefix), sstableFileSuffix)
	id, _ := strconv.ParseUint(idPart, 10, 64)
	return id
}
