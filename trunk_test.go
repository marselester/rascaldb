package rascaldb

import "testing"

func TestReadSegmentNames(t *testing.T) {
	segments, err := readSegmentNames("testdata/readtrunk.txt")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"oldest", "newest"}
	if !equal(segments, want) {
		t.Errorf("readSegmentNames() got %v, want %v", segments, want)
	}
}

func TestWriteSegmentNames(t *testing.T) {
	segments := []string{"fizz", "bazz"}
	err := writeSegmentNames("testdata/writetrunk.txt", segments)
	if err != nil {
		t.Fatal(err)
	}

	got, err := readSegmentNames("testdata/writetrunk.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !equal(got, segments) {
		t.Errorf("writeSegmentNames() wrote %q, want %q", got, segments)
	}

	teardown()
}
