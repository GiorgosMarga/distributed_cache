package cache

import (
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Data struct {
	Value      []byte
	Ttl        uint32
	ValidUntil time.Time
}
type Cache interface {
	Set([]byte, []byte, uint32) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
	GetData() map[string]*Data
}
