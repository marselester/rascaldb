// Package rascaldb is a key-value log-structured storage engine with hash map index.
package rascaldb

import (
	"context"
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

	// DB is a state machine which requires various concurrent actions, for example,
	// put new keys, rotate segments when the current one becomes too big.
	// Actions are executed in run method which acts as a serialization point;
	// have a look at Actor stuff https://speakerdeck.com/peterbourgon/ways-to-do-things.
	actionsc chan func()
	// cancel signals the actor to stop.
	cancel context.CancelFunc
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
	db := &DB{
		name:     name,
		actionsc: make(chan func()),
	}
	db.segments.Store(
		make([]*segment, 0, segmentsQty),
	)

	// Make sure nobody is updating segments slice.
	db.mu.Lock()
	defer db.mu.Unlock()
	ss := db.segments.Load().([]*segment)
	var s *segment
	for _, segName := range filenames {
		path = filepath.Join(db.name, segName)
		if s, err = openSegment(path, false); err != nil {
			return nil, err
		}
		if err = s.loadIndex(); err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	// Create a new segment for writes.
	path = filepath.Join(db.name, "some-new-name")
	if s, err = openSegment(path, true); err != nil {
		return nil, err
	}
	ss = append(ss, s)
	// Atomically replace the current segments slice with the new one.
	// At this point all new readers start working with the new version.
	// The old version will be garbage collected once the existing readers (if any) are done with it.
	db.segments.Store(ss)

	ctx, cancel := context.WithCancel(context.Background())
	db.cancel = cancel
	go db.run(ctx)

	return db, nil
}

// Close closes database resources.
func (db *DB) Close() {
	// All segment files are closed...
	// The state machine's loop is stopped.
	db.cancel()
}

// run executes every function from actionsc and acts as a serialization point.
// It doesn't know about business logic.
func (db *DB) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-db.actionsc:
			f()
		}
	}
}

// Set puts a key in database. You can call it concurrently.
func (db *DB) Set(key string, value []byte) error {
	errc := make(chan error)

	db.actionsc <- func() {
		ss := db.segments.Load().([]*segment)
		current := ss[len(ss)-1]
		errc <- current.write(key, value)
	}

	return <-errc
}

// Get retrieves a key from database. You can call it concurrently.
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
