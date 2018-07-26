# RascalDB 😜

[![Documentation](https://godoc.org/github.com/marselester/rascaldb?status.svg)](https://godoc.org/github.com/marselester/rascaldb)
[![Go Report Card](https://goreportcard.com/badge/github.com/marselester/rascaldb)](https://goreportcard.com/report/github.com/marselester/rascaldb)

RascalDB is a key-value log-structured storage engine with hash map index.
All keys must fit in RAM since the hash map is kept in memory.
This type of a storage is optimal for writes workload.

Note, this pet project is not intended for production use.

Key ideas:

- [x] key-values are immutable, appended to a log
- [x] log is represented as a sequence of segment files
- [x] key-value is stored as a record prefixed with its length (4 bytes)
- [x] key is looked up from segment files using in-memory hash map index
  which maintains a byte offset of a key
- [x] hash map index is loaded from a segment file when db is opened
- [x] sequence of database segments is stored in a trunk file
- [ ] old log segments are compacted (old records of duplicate keys are removed)
- [ ] old segments are merged
- [x] there is only one writer to make sure keys are written linearly
- [ ] ignore corrupted segments if a file's checksum doesn't match when db crashed

## Usage Example

```go
package main

import (
	"fmt"
	"log"

	"github.com/marselester/rascaldb"
)

func main() {
	db, err := rascaldb.Open("my.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	name := []byte("Moist von Lipwig")
	if err = db.Set("name", name); err != nil {
		log.Fatal(err)
	}

	if name, err = db.Get("name"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", name)
}
```
