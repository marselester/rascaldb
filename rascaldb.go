// Package rascaldb is a key-value log-structured storage engine with hash map index.
package rascaldb

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// DB represents RascalDB database on disk, created by Open.
type DB struct {
	// name is a dir where segment files are stored.
	name string
	// mu mutex is used only to modify segments slice.
	mu sync.Mutex
	// segments is a slice of segment files where records are stored.
	// Oldest segments are in the beginning of the slice.
	segments atomic.Value
}

// Open opens a database with the specified name.
// If a database doesn't exist, it will be created. Database is a dir where segment files are kept.
func Open(name string) (*DB, error) {
	if err := os.MkdirAll(name, 0700); err != nil {
		return nil, err
	}

	path := filepath.Join(name, trunk)
	filenames, err := readSegmentNames(path)
	// Since there is no trunk file, this is a new db.
	// Let's make sure it's writable.
	if os.IsNotExist(err) {
		err = writeSegmentNames(path, nil)
	}
	if err != nil {
		return nil, err
	}

	// All existing segment files are opened for read only and a new segment is writable.
	segmentsQty := len(filenames) + 1
	db := &DB{name: name}
	db.segments.Store(
		make([]*segment, 0, segmentsQty),
	)

	var s *segment
	for _, segName := range filenames {
		path = filepath.Join(name, segName)
		if s, err = openSegment(path, false); err != nil {
			return nil, err
		}
		if err = s.loadIndex(); err != nil {
			return nil, err
		}

		// Make sure nobody is updating segments slice.
		db.mu.Lock()
		ss := db.segments.Load().([]*segment)
		ss = append(ss, s)
		// Atomically replace the current segments slice with the new one.
		// At this point all new readers start working with the new version.
		// The old version will be garbage collected once the existing readers (if any) are done with it.
		db.segments.Store(ss)
		db.mu.Unlock()
	}

	return db, nil
}

// Close closes database. All segment files are closed.
func (db *DB) Close() {}

// Get retrieves a key from database.
func (db *DB) Get(key string) ([]byte, error) {
	ss := db.segments.Load().([]*segment)

	var ok bool
	var offset int64
	for i := len(ss) - 1; i >= 0; i-- {
		if offset, ok = ss[i].index[key]; ok {
			_, value, err := ss[i].read(offset)
			return value, err
		}
	}

	return nil, ErrKeyNotFound
}

// Set puts a key in database.
func (db *DB) Set(key string, value []byte) error {
	return nil
}
