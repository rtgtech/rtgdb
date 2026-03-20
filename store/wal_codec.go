package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	OpPut byte = 1 + iota
	OpDelete
)

var (
	ErrRecordTooLarge = errors.New("wal record too large")
	ErrUnknownOp      = errors.New("unknown wal operation")
	ErrPartialRecord  = errors.New("partial wal record")
	ErrCRCMismatch    = errors.New("wal crc mismatch")
)

type Record struct {
	Op    byte
	Key   string
	Value []byte
}

func EncodePut(key string, value []byte) ([]byte, error) {
	return encodeRecord(OpPut, key, value)
}

func EncodeDelete(key string) ([]byte, error) {
	return encodeRecord(OpDelete, key, nil)
}

func encodeRecord(op byte, key string, value []byte) ([]byte, error) {
	if err := ValidateKey(key); err != nil {
		return nil, err
	}
	if op != OpPut && op != OpDelete {
		return nil, ErrUnknownOp
	}

	keyBytes := []byte(key)
	if len(keyBytes) > int(^uint32(0)) || len(value) > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}

	recordLen := 1 + 4 + len(keyBytes) + 4 + len(value)
	if recordLen > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}

	buf := make([]byte, 4+recordLen+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(recordLen))

	payload := buf[4 : 4+recordLen]
	payload[0] = op
	binary.BigEndian.PutUint32(payload[1:5], uint32(len(keyBytes)))
	copy(payload[5:5+len(keyBytes)], keyBytes)

	valueLenOffset := 5 + len(keyBytes)
	binary.BigEndian.PutUint32(payload[valueLenOffset:valueLenOffset+4], uint32(len(value)))
	copy(payload[valueLenOffset+4:], value)

	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(buf[4+recordLen:], crc)
	return buf, nil
}

func DecodeRecord(r io.Reader) (Record, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, ErrPartialRecord
		}
		return Record{}, err
	}

	recordLen := binary.BigEndian.Uint32(lenBuf[:])
	payload := make([]byte, recordLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return Record{}, ErrPartialRecord
		}
		return Record{}, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return Record{}, ErrPartialRecord
		}
		return Record{}, err
	}

	storedCRC := binary.BigEndian.Uint32(crcBuf[:])
	computedCRC := crc32.ChecksumIEEE(payload)
	if storedCRC != computedCRC {
		return Record{}, ErrCRCMismatch
	}

	record, err := parsePayload(payload)
	if err != nil {
		return Record{}, err
	}
	return record, nil
}

func parsePayload(payload []byte) (Record, error) {
	if len(payload) < 9 {
		return Record{}, ErrPartialRecord
	}

	op := payload[0]
	if op != OpPut && op != OpDelete {
		return Record{}, ErrUnknownOp
	}

	keyLen := int(binary.BigEndian.Uint32(payload[1:5]))
	keyStart := 5
	keyEnd := keyStart + keyLen
	if len(payload) < keyEnd+4 {
		return Record{}, ErrPartialRecord
	}

	key := string(payload[keyStart:keyEnd])
	if err := ValidateKey(key); err != nil {
		return Record{}, err
	}

	valueLen := int(binary.BigEndian.Uint32(payload[keyEnd : keyEnd+4]))
	valueStart := keyEnd + 4
	valueEnd := valueStart + valueLen
	if len(payload) != valueEnd {
		return Record{}, ErrPartialRecord
	}

	value := cloneBytes(payload[valueStart:valueEnd])
	if op == OpDelete && valueLen != 0 {
		return Record{}, ErrPartialRecord
	}

	return Record{
		Op:    op,
		Key:   key,
		Value: value,
	}, nil
}
