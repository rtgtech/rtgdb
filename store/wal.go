package store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type SyncMode string

const (
	SyncAlways SyncMode = "always"
	SyncNever  SyncMode = "never"
)

var ErrInvalidSyncMode = errors.New("invalid wal sync mode")

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	syncMode SyncMode
}

func OpenWAL(path string, syncMode SyncMode) (*WAL, error) {
	if syncMode == "" {
		syncMode = SyncAlways
	}
	if syncMode != SyncAlways && syncMode != SyncNever {
		return nil, ErrInvalidSyncMode
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:     file,
		syncMode: syncMode,
	}, nil
}

func (w *WAL) Append(record []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Write(record); err != nil {
		return err
	}
	if w.syncMode == SyncAlways {
		return w.file.Sync()
	}
	return nil
}

func (w *WAL) Replay(apply func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		record, err := DecodeRecord(w.file)
		if err == nil {
			if err := apply(record); err != nil {
				return err
			}
			continue
		}

		switch {
		case errors.Is(err, io.EOF):
			_, seekErr := w.file.Seek(0, io.SeekEnd)
			return seekErr
		case errors.Is(err, ErrPartialRecord), errors.Is(err, ErrCRCMismatch):
			_, seekErr := w.file.Seek(0, io.SeekEnd)
			return seekErr
		default:
			return err
		}
	}
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	if w.syncMode == SyncAlways {
		if err := w.file.Sync(); err != nil {
			_ = w.file.Close()
			w.file = nil
			return err
		}
	}
	err := w.file.Close()
	w.file = nil
	return err
}
