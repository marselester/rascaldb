package rascaldb

import (
	"bytes"
	"os"
	"testing"
)

// Make sure testdata is cleared after the tests run.
func TestMain(m *testing.M) {
	code := m.Run()
	teardown()
	os.Exit(code)
}

func teardown() {
	os.Remove("testdata/404segment")
	os.Remove("testdata/writesegment")
	os.Remove("testdata/writetrunk.txt")
	os.RemoveAll("testdata/new.db")
}

func equal(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func TestOpen_new(t *testing.T) {
	dbpath := "testdata/new.db"
	trunkpath := "testdata/new.db/trunk.txt"
	db, err := Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if db.name != dbpath {
		t.Errorf("Open(%q) got %q, want %q", dbpath, db.name, dbpath)
	}

	_, err = os.Stat(trunkpath)
	if os.IsNotExist(err) {
		t.Errorf("Open(%q) trunk file %q not created", dbpath, trunkpath)
	}

	segments := db.segments.Load().([]*segment)
	wantLen := 1
	if len(segments) != wantLen {
		t.Errorf("Open(%q) got segments %d, want %d", dbpath, len(segments), wantLen)
	}
}

func TestOpen_existing(t *testing.T) {
	dbpath := "testdata/read.db"
	db, err := Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if db.name != dbpath {
		t.Errorf("Open(%q) got %q, want %q", dbpath, db.name, dbpath)
	}

	segments := db.segments.Load().([]*segment)
	wantLen := 2
	if len(segments) != wantLen {
		t.Errorf("Open(%q) got segments %d, want %d", dbpath, len(segments), wantLen)
	}

	wantName := "testdata/read.db/oldsegment"
	if segments[0].name != wantName {
		t.Errorf("Open(%q) got first segment %q, want %q", dbpath, segments[0].name, wantName)
	}
	if _, ok := segments[0].index["name"]; !ok {
		t.Errorf("Open(%q) first segment %q index is not loaded", dbpath, segments[0].name)
	}

	wantName = "testdata/read.db/newsegment"
	if segments[1].name != wantName {
		t.Errorf("Open(%q) got second segment %q, want %q", dbpath, segments[1].name, wantName)
	}
	if _, ok := segments[1].index["nick"]; !ok {
		t.Errorf("Open(%q) second segment %q index is not loaded", dbpath, segments[1].name)
	}
}

func TestDB_Get(t *testing.T) {
	dbpath := "testdata/read.db"
	db, err := Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tt := []struct {
		key       string
		wantValue []byte
		wantErr   error
	}{
		{"name", []byte("Rob"), nil},
		{"nick", []byte("B0B"), nil},
		{"unknown", nil, ErrKeyNotFound},
		{"", nil, ErrKeyNotFound},
		{" ", nil, ErrKeyNotFound},
		{"\x00", nil, ErrKeyNotFound},
	}

	for _, tc := range tt {
		got, err := db.Get(tc.key)
		if err != tc.wantErr {
			t.Errorf("Get(%q) error %v, want %v", tc.key, err, tc.wantErr)
		}
		if !bytes.Equal(got, tc.wantValue) {
			t.Errorf("Get(%q) = %q, want %q", tc.key, got, tc.wantValue)
		}
	}
}
