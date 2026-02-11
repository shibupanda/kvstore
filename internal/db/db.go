package db

import (
	"errors"
	"fmt"
	"io"
	"kvstore/internal/log"
)

type DB struct {
	log   *log.LogFile
	index *Index
}

func Open(path string) (*DB, error) {
	lf, err := log.OpenLogFile(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		log:   lf,
		index: NewIndex(),
	}

	// Rebuild index by scanning full log
	err = db.rebuildIndex()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) rebuildIndex() error {
	_, err := db.log.F.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var offset int64 = 0

	for {
		key, value, length, err := db.log.ReadNextRecord()
		if err == io.EOF {
			break
		}
		if err == log.ErrCorruptRecord {
			fmt.Println("Warning: corrupted record at offset", offset)
			break
		}
		if err != nil {
			return err
		}

		// Tombstone (valueLen = 0 means our delete)
		if len(value) == 0 {
			db.index.Delete(key)
		} else {
			db.index.Set(key, offset, length)
		}

		offset += int64(length)
	}

	return nil
}

func (db *DB) Put(key string, value []byte) error {
	offset, length, err := db.log.WriteRecord([]byte(key), value)
	if err != nil {
		return err
	}

	db.index.Set(key, offset, length)
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	entry, ok := db.index.Get(key)
	if !ok {
		return nil, nil
	}

	k, v, err := db.log.ReadRecordAt(entry.Offset)
	if err == log.ErrCorruptRecord {
		return nil, errors.New("data corrupted")
	}
	if err != nil {
		return nil, err
	}

	if k != key {
		return nil, nil
	}

	return v, nil
}

func (db *DB) Delete(key string) error {
	// Tombstone = empty value
	offset, length, err := db.log.WriteRecord([]byte(key), []byte{})
	if err != nil {
		return err
	}

	db.index.Delete(key)
	_ = offset
	_ = length

	return nil
}
