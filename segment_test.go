package rascaldb

import (
	"io"
	"io/ioutil"
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
	os.Remove("testdata/notfound.db")
	os.Remove("testdata/writetest.db")
}

func TestOpenSegment_error(t *testing.T) {
	tt := []struct {
		name    string
		file    string
		current bool
		wantErr bool
	}{
		{"write ok", "testdata/notfound.db", true, false},
		{"write error", "testdata/readtest.db", true, false},
		{"read error", "testdata/notfound.db", false, true},
		{"read ok", "testdata/readtest.db", false, false},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			_, err := openSegment(tc.file, tc.current)
			if _, ok := err.(*os.PathError); ok != tc.wantErr {
				t.Errorf("openSegment(%q, %t) = %v, want %v", tc.file, tc.current, err, tc.wantErr)
			}

			teardown()
		})
	}
}

func TestSegment_close(t *testing.T) {
	s := segment{}
	if err := s.close(); err != nil {
		t.Errorf("close error when both files are nil: %v", err)
	}

	f, err := os.Open("testdata/readtest.db")
	s = segment{fr: f}
	if err != nil {
		t.Fatal(err)
	}
	if err = s.close(); err != nil {
		t.Errorf("close error when read file is open: %v", err)
	}

	f, err = os.Open("testdata/readtest.db")
	s = segment{fw: f}
	if err != nil {
		t.Fatal(err)
	}
	if err = s.close(); err != nil {
		t.Errorf("close error when write file is open: %v", err)
	}
}

func TestSegment_read(t *testing.T) {
	s, err := openSegment("testdata/readtest.db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	tt := []struct {
		name      string
		offset    int64
		wantKey   string
		wantValue []byte
	}{
		{
			name:      "ok read first pair",
			offset:    0,
			wantKey:   "name",
			wantValue: []byte("Bob"),
		},
		{
			name:      "ok read second pair",
			offset:    12,
			wantKey:   "name",
			wantValue: []byte("Jon"),
		},
		{
			name:      "empty wrong offset",
			offset:    1,
			wantKey:   "",
			wantValue: nil,
		},
		{
			name:      "empty offset out of range",
			offset:    100,
			wantKey:   "",
			wantValue: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			key, value, _ := s.read(tc.offset)
			if key != tc.wantKey {
				t.Errorf("read(%d) key %q, want %q", tc.offset, key, tc.wantKey)
			}
			if !equal(value, tc.wantValue) {
				t.Errorf("read(%d) value %q, want %q", tc.offset, value, tc.wantValue)
			}
		})
	}
}

func TestSegment_read_error(t *testing.T) {
	s, err := openSegment("testdata/readtest.db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	tt := []struct {
		name   string
		offset int64
		err    error
	}{
		{
			name:   "ok first record",
			offset: 0,
			err:    nil,
		},
		{
			name:   "ok second record",
			offset: 12,
			err:    nil,
		},
		{
			name:   "err wrong offset",
			offset: 1,
			err:    io.EOF,
		},
		{
			name:   "err offset out of range",
			offset: 100,
			err:    io.EOF,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err = s.read(tc.offset); err != tc.err {
				t.Errorf("read(%d) got %v, want %v", tc.offset, err, tc.err)
			}
		})
	}
}

func TestSegment_write(t *testing.T) {
	s, err := openSegment("testdata/writetest.db", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	key := "name"
	value := []byte("Bob")
	if err = s.write(key, value); err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadFile("testdata/writetest.db")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("\f\x00\x00\x00name\x00Bob")
	if !equal(got, want) {
		t.Errorf("write(%q, %q) got %q, want %q", key, value, got, want)
	}

	var wantOffset int64
	if s.index[key] != wantOffset {
		t.Errorf("write(%q, %q) key offset %d, want %d", key, value, s.index[key], wantOffset)
	}

	// 4 bytes for record len, key is 4 bytes, delimeter is 1 byte, value is 3 bytes.
	wantOffset = 12
	if s.offset != wantOffset {
		t.Errorf("write(%q, %q) new offset %d, want %d", key, value, s.offset, wantOffset)
	}
}

func TestSegment_loadIndex(t *testing.T) {
	s, err := openSegment("testdata/readtest.db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	if err := s.loadIndex(); err != nil {
		t.Errorf("loadIndex error: %v", err)
	}

	key := "name"
	offset, ok := s.index[key]
	if !ok {
		t.Errorf("loadIndex %q key is not indexed", key)
	}

	const want = 12
	if offset != want {
		t.Errorf("loadIndex %q key offset is %d, want %d", key, offset, want)
	}
}

func TestEncode(t *testing.T) {
	tt := []struct {
		key   string
		value []byte
		want  []byte
	}{
		{
			// [110 97 109 101]
			key: "name",
			// [66 111 98]
			value: []byte("Bob"),
			// record len (4 bytes) + key + delimeter (1 byte) + value
			want: []byte{12, 0, 0, 0, 110, 97, 109, 101, 0, 66, 111, 98},
		},
	}

	for _, tc := range tt {
		got := encode(tc.key, tc.value)
		if !equal(got, tc.want) {
			t.Errorf("encode(%q, %v) = %v, want %v", tc.key, tc.value, got, tc.want)
		}
	}
}

func TestDecode(t *testing.T) {
	tt := []struct {
		b         []byte
		wantKey   string
		wantValue []byte
	}{
		{
			b:         []byte{12, 0, 0, 0, 110, 97, 109, 101, 0, 66, 111, 98},
			wantKey:   "name",
			wantValue: []byte("Bob"),
		},
	}

	for _, tc := range tt {
		key, value := decode(tc.b)
		if key != tc.wantKey {
			t.Errorf("decode(%q) key %q, want %q", tc.b, key, tc.wantKey)
		}
		if !equal(value, tc.wantValue) {
			t.Errorf("read(%q) value %q, want %q", tc.b, value, tc.wantValue)
		}
	}
}

func equal(s1, s2 []byte) bool {
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
