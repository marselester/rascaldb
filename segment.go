package rascaldb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// recordLenSize is a record length in bytes needed to encode uint32.
const recordLenSize = 4

// kvDelimeter is a delimiter between key and value in segment's record.
const kvDelimeter = byte('\x00')

// segment represents a log file (append-only sequence of records).
type segment struct {
	// name is a segment's filename including the db dir.
	name string
	// fw is a File opened for writing logs.
	fw *os.File
	// fr is a File opened for reads.
	fr *os.File
	// offset is an offset of the latest record appended to the file.
	offset int64
	// index is a hash map which is used to index keys on disk.
	// Every key is mapped to a byte offset in the segment file where value is stored.
	index map[string]int64
}

// openSegment opens a segment file for reads and writes if the segment is writable.
// Note, you must call loadIndex to populate in-memory index.
func openSegment(name string, writable bool) (*segment, error) {
	s := segment{
		name:  name,
		index: make(map[string]int64),
	}

	var err error
	if writable {
		// By design we don't modify existing files.
		if s.fw, err = os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0600); err != nil {
			return &s, err
		}
	}
	s.fr, err = os.Open(name)
	return &s, err
}

// Close closes a segment file which was opened for reads and maybe writes.
func (s *segment) close() error {
	if s.fr != nil {
		s.fr.Close()
	}
	if s.fw != nil {
		return s.fw.Close()
	}
	return nil
}

// read reads a key-value pair by the offset from the segment file.
func (s *segment) read(offset int64) (string, []byte, error) {
	recordLen := make([]byte, recordLenSize)
	if _, err := s.fr.ReadAt(recordLen, offset); err != nil {
		return "", nil, err
	}
	blen := binary.LittleEndian.Uint32(recordLen)

	b := make([]byte, blen)
	if _, err := s.fr.ReadAt(b, offset); err != nil {
		return "", nil, err
	}

	key, value := decode(b)
	return key, value, nil
}

// write appends a key-value pair to a log file and updates the index.
// Note, it is not concurrency safe. By design there should be only one writer.
func (s *segment) write(key string, value []byte) error {
	n, err := s.fw.Write(encode(key, value))
	if err != nil {
		return err
	}
	if err := s.fw.Sync(); err != nil {
		return err
	}
	s.index[key] = s.offset
	s.offset += int64(n)
	return nil
}

// loadIndex loads keys from the segment file into in-memory index.
// Note, it is not concurrency safe since it touches the index.
func (s *segment) loadIndex() error {
	var offset int64
	for {
		switch key, value, err := s.read(offset); err {
		case nil:
			s.index[key] = offset
			offset += int64(recordLen(key, value))
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

// encode prepares the key value pair to be stored in a file.
// First 4 bytes store the length of a record. The rest of bytes are key-value (zero byte is used as a delimeter).
func encode(key string, value []byte) []byte {
	blen := recordLen(key, value)
	b := make([]byte, recordLenSize, blen)

	binary.LittleEndian.PutUint32(b, blen)
	b = append(b, key...)
	b = append(b, kvDelimeter)
	b = append(b, value...)
	return b
}

// decode returns key-value from encoded byte slice b.
func decode(b []byte) (string, []byte) {
	b = b[recordLenSize:]
	i := bytes.IndexByte(b, kvDelimeter)
	if i == -1 {
		return "", nil
	}

	key := string(b[0:i])
	value := b[i+1:] // Skip delimeter and read till the end.
	return key, value
}

// recordLen is used to read next record in a segment file.
// Max record len is 4,294,967,295 (4.295 GB).
// For example, start from 0 offset, read key-value pair, move to offset += recordLen(key, value).
func recordLen(key string, value []byte) uint32 {
	return recordLenSize + uint32(len(key)) + 1 + uint32(len(value))
}
