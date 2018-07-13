package rascaldb

// ErrKeyNotFound is returned when a requested key is not found in database.
const ErrKeyNotFound = Error("key not found")

// Error defines RascalDB errors.
type Error string

func (e Error) Error() string {
	return string(e)
}
