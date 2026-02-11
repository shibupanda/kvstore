package log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type LogFile struct {
	F *os.File
}

// OpenLogFile opens/creates an append-only log
func OpenLogFile(path string) (*LogFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &LogFile{F: f}, nil
}

// WriteRecord writes one record to the log:
// [crc32][keyLen][valueLen][key][value]
func (lf *LogFile) WriteRecord(key, value []byte) (int64, int, error) {
	offset, _ := lf.F.Seek(0, io.SeekEnd)
	keyLen := int32(len(key))
	valueLen := int32(len(value))
	
	// Compute CRC over key + value
	crc := crc32.ChecksumIEEE(append(key, value...))

	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:], crc)
	binary.LittleEndian.PutUint32(header[4:], uint32(keyLen))
	binary.LittleEndian.PutUint32(header[8:], uint32(valueLen))

	totalLen := 12 + len(key) + len(value)

	_, err := lf.F.Write(header)
	if err != nil {
		return 0, 0, nil
	}

	_, err = lf.F.Write(key)
	if err != nil {
		return 0, 0, nil
	}

	_, err = lf.F.Write(value)
	if err != nil {
		return 0, 0, err
	}

	return offset, totalLen, nil
}

// ReadRecortAt reads a record at a specific offset
func (lf *LogFile) ReadRecordAt(offset int64) (string, []byte, error) {
	_,err := lf.F.Seek(offset, io.SeekStart)
	if err != nil {
		return "", nil, err
	}

	header := make([]byte, 12)
	_, err = lf.F.Read(header)
	if err != nil {
		return "", nil, err
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:])
	keyLen := int(binary.LittleEndian.Uint32(header[4:]))
	valueLen := int(binary.LittleEndian.Uint32(header[8:]))

	key := make([]byte, keyLen)
	_, err = lf.F.Read(key)
	if err != nil {
		return "", nil, err
	}

	value := make([]byte, valueLen)
	_, err = lf.F.Read(value)
	if err != nil {
		return "", nil, err
	}

	computed := crc32.ChecksumIEEE(append(key, value...))
	if computed != storedCRC {
		return "", nil, ErrCorruptRecord
	}

	return string(key), value, nil
}

// ReadNextRecord reads a record at the current cursor during index rebuild.
func (lf *LogFile) ReadNextRecord() (string, []byte, int, error) {
	header := make([]byte, 12)

	n, err := lf.F.Read(header)
	if err != nil {
		return "", nil, 0, err
	}
	if n < 12 {
		return "", nil, 0, io.EOF
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:])
	keyLen := int(binary.LittleEndian.Uint32(header[4:]))
	valueLen := int(binary.LittleEndian.Uint32(header[8:]))

	key := make([]byte, keyLen)
	_, err = lf.F.Read(key)
	if err != nil {
		return "", nil, 0, err
	}

	value := make([]byte, valueLen)
	_, err = lf.F.Read(value)
	if err != nil {
		return "", nil, 0, err
	}

	// Validate CRC
	computed := crc32.ChecksumIEEE(append(key, value...))
	if computed != storedCRC {
		return "", nil, 0, ErrCorruptRecord
	}

	recLen := 12 + keyLen + valueLen
	return string(key), value, recLen, nil
}

var ErrCorruptRecord = fmt.Errorf("corrupted record")

