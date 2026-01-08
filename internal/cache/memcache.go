package cache

import (
	"fmt"
	"sync"
	"time"
)

type MemCache struct {
	totalCapacity uint32
	maxCapacity   uint32
	cache         map[string][]byte
	mtx           *sync.RWMutex
}

func NewMemCache(maxCapacity uint32) *MemCache {
	return &MemCache{
		totalCapacity: 0,
		maxCapacity:   maxCapacity,
		mtx:           &sync.RWMutex{},
		cache:         make(map[string][]byte, maxCapacity),
	}
}

func (mc *MemCache) Set(key, value []byte, ttl uint32) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	mc.cache[string(key)] = value

	go func(k []byte) {
		timer := time.NewTimer(time.Duration(ttl) * time.Second)
		for range timer.C {
			if err := mc.Delete(k); err != nil {
				fmt.Printf("[MemCache]: Error deleting: %s\n", string(key))
			}
		}
	}(key)
	return nil
}

func (mc *MemCache) Get(key []byte) ([]byte, error) {
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()

	v, ok := mc.cache[string(key)]
	if !ok {
		return nil, fmt.Errorf("[MemCache]: [%s] %w", string(key), ErrNotFound)
	}

	return v, nil
}

func (mc *MemCache) Delete(key []byte) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()
	if _, ok := mc.cache[string(key)]; !ok {
		return fmt.Errorf("[MemCache]: [%s] %w", string(key), ErrNotFound)
	}
	delete(mc.cache, string(key))
	return nil
}
