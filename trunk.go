package rascaldb

import (
	"bufio"
	"os"
)

// trunk is a file where the list of segment filenames is stored.
// That way we know in which order segments should be traversed when looking for a key.
// Oldest segments are in the beginning of the list.
const trunk = "trunk.txt"

// readSegmentNames returns a slice of segment filenames stored in a special trunk file (sequence of segments).
// That way we know in which order segments should be traversed when looking for a key.
func readSegmentNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var files []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		files = append(files, scanner.Text())
	}
	return files, scanner.Err()
}

// writeSegmentNames stores a slice of segment filenames in a special trunk file (sequence of segments).
// That way we know in which order segments should be traversed when looking for a key.
func writeSegmentNames(path string, names []string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	for _, segName := range names {
		if _, err = buf.WriteString(segName + "\n"); err != nil {
			return err
		}
	}

	if err = buf.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}
