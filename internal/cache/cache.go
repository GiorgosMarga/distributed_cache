package cache

import "errors"

var (
	ErrNotFound = errors.New("key not found")
)

type Cache interface {
	Set([]byte, []byte, uint32) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
}
