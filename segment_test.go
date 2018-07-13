package rascaldb

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpenSegment_error(t *testing.T) {
	tt := []struct {
		name    string
		file    string
		current bool
		wantErr bool
	}{
		{"write ok", "testdata/404segment", true, false},
		{"write error", "testdata/readsegment", true, false},
		{"read error", "testdata/404segment", false, true},
		{"read ok", "testdata/readsegment", false, false},
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
		t.Errorf("close() error when both files are nil: %v", err)
	}

	f, err := os.Open("testdata/readsegment")
	s = segment{fr: f}
	if err != nil {
		t.Fatal(err)
	}
	if err = s.close(); err != nil {
		t.Errorf("close() error when read file is open: %v", err)
	}

	f, err = os.Open("testdata/readsegment")
	s = segment{fw: f}
	if err != nil {
		t.Fatal(err)
	}
	if err = s.close(); err != nil {
		t.Errorf("close() error when write file is open: %v", err)
	}
}

func TestSegment_read(t *testing.T) {
	s, err := openSegment("testdata/readsegment", false)
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
			if !bytes.Equal(value, tc.wantValue) {
				t.Errorf("read(%d) value %q, want %q", tc.offset, value, tc.wantValue)
			}
		})
	}
}

func TestSegment_read_error(t *testing.T) {
	s, err := openSegment("testdata/readsegment", false)
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
	tt := []struct {
		name           string
		key            string
		value          []byte
		wantRecord     []byte
		wantOffset     int64
		wantNextOffset int64
	}{
		{
			name:       "name=Bob",
			key:        "name",
			value:      []byte("Bob"),
			wantRecord: []byte("\f\x00\x00\x00name\x00Bob"),
			wantOffset: 0,
			// 4 bytes for record len, key is 4 bytes, delimeter is 1 byte, value is 3 bytes.
			wantNextOffset: 12,
		},
		{
			name:       "name=nil",
			key:        "name",
			value:      nil,
			wantRecord: []byte("\t\x00\x00\x00name\x00"),
			wantOffset: 0,
			// 4 bytes for record len, key is 4 bytes, delimeter is 1 byte, value is 0 bytes.
			wantNextOffset: 9,
		},
		{
			name:       "empty=Bob",
			key:        "",
			value:      []byte("Bob"),
			wantRecord: []byte("\b\x00\x00\x00\x00Bob"),
			wantOffset: 0,
			// 4 bytes for record len, key is 0 bytes, delimeter is 1 byte, value is 3 bytes.
			wantNextOffset: 8,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			s, err := openSegment("testdata/writesegment", true)
			if err != nil {
				t.Fatal(err)
			}

			if err = s.write(tc.key, tc.value); err != nil {
				t.Fatal(err)
			}
			s.close()

			record, err := ioutil.ReadFile("testdata/writesegment")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(record, tc.wantRecord) {
				t.Errorf("write(%q, %q) got %q, want %q", tc.key, tc.value, record, tc.wantRecord)
			}

			if s.index[tc.key] != tc.wantOffset {
				t.Errorf("write(%q, %q) key offset %d, want %d", tc.key, tc.value, s.index[tc.key], tc.wantOffset)
			}

			if s.offset != tc.wantNextOffset {
				t.Errorf("write(%q, %q) new offset %d, want %d", tc.key, tc.value, s.offset, tc.wantNextOffset)
			}

			teardown()
		})
	}
}

func TestSegment_loadIndex(t *testing.T) {
	s, err := openSegment("testdata/readsegment", false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	if err := s.loadIndex(); err != nil {
		t.Errorf("loadIndex() error: %v", err)
	}

	key := "name"
	offset, ok := s.index[key]
	if !ok {
		t.Errorf("loadIndex() %q key is not indexed", key)
	}

	const want = 12
	if offset != want {
		t.Errorf("loadIndex() %q key offset is %d, want %d", key, offset, want)
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
		if !bytes.Equal(got, tc.want) {
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
		if !bytes.Equal(value, tc.wantValue) {
			t.Errorf("decode(%q) value %q, want %q", tc.b, value, tc.wantValue)
		}
	}
}
