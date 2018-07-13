# RascalDB 😜

[![Documentation](https://godoc.org/github.com/marselester/rascaldb?status.svg)](https://godoc.org/github.com/marselester/rascaldb)
[![Go Report Card](https://goreportcard.com/badge/github.com/marselester/rascaldb)](https://goreportcard.com/report/github.com/marselester/rascaldb)

RascalDB is a key-value log-structured storage engine with hash map index.
All keys must fit in RAM since the hash map is kept in memory.

Note, this pet project is not intended for production use.

## Usage Example

```go
db, err := rascaldb.Open("my.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

if err = db.Set("name", []byte("Moist von Lipwig")); err != nil {
    log.Fatal(err)
}

if _, err := db.Get("name"); err == rascaldb.ErrKeyNotFound {
    fmt.Println("name was not found")
}
```
