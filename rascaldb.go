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
	// segmentNamer is a function that returns random segment names.
	segmentNamer func() string
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
	// quitc signals the actor to stop.
	quitc chan struct{}
}

// Open opens a database with the specified name.
// If a database doesn't exist, it will be created. Database is a dir where segment files are kept.
func Open(name string) (*DB, error) {
	db := DB{
		name:         name,
		segmentNamer: newSegmentNamer(),
		actionsc:     make(chan func()),
		quitc:        make(chan struct{}),
	}
	if err := os.MkdirAll(db.name, 0700); err != nil {
		return nil, err
	}

	path := filepath.Join(db.name, trunk)
	filenames, err := readSegmentNames(path)
	// Since there is no trunk file, this is a new database.
	// Let's create the first segment and store its name in the trunk.
	if os.IsNotExist(err) {
		filenames = append(filenames, db.segmentNamer())
		err = writeSegmentNames(path, filenames)
	}
	if err != nil {
		return nil, err
	}

	ss := make([]*segment, 0, len(filenames))
	var s *segment
	// Open segments for reads, load indexes. The last segment is opened for writes.
	for i, segName := range filenames {
		isLast := i == len(filenames)-1
		path = filepath.Join(db.name, segName)
		if s, err = openSegment(path, isLast); err != nil {
			return nil, err
		}
		if err = s.loadIndex(); err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	db.segments.Store(ss)

	go db.run()
	return &db, nil
}

// Close closes database resources.
func (db *DB) Close() {
	// The state machine's loop is stopped.
	close(db.quitc)
	// All segment files are closed.
	ss := db.segments.Load().([]*segment)
	for _, s := range ss {
		s.close()
	}
}

// run executes every function from actionsc and acts as a serialization point.
// It doesn't know about business logic.
func (db *DB) run() {
	for {
		select {
		case f := <-db.actionsc:
			f()
		case <-db.quitc:
			return
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
