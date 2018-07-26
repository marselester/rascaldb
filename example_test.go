package rascaldb_test

import (
	"fmt"
	"log"

	"github.com/marselester/rascaldb"
)

func Example() {
	db, err := rascaldb.Open("testdata/new.db")
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
	// Output:
	// Moist von Lipwig
}
