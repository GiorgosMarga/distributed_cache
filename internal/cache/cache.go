package cache

import (
	"errors"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Data struct {
	Key        []byte
	Value      []byte
	Ttl        uint32
	InsertedAt int64
}
type Cache interface {
	Set(*Data) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
	GetData() []*Data
	Stop()
}
