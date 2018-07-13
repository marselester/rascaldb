package rascaldb

import (
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
