package cache

import (
	"fmt"
	"sync"
	"time"
)

const (
	DeleteInterval = 1000 // in ms
)

type MemCache struct {
	totalCapacity uint32
	maxCapacity   uint32
	cache         map[string][]byte
	ttls          map[string]time.Time
	mtx           *sync.RWMutex
	quitChan      chan struct{}
}

func NewMemCache(maxCapacity uint32) *MemCache {
	mc := &MemCache{
		totalCapacity: 0,
		maxCapacity:   maxCapacity,
		mtx:           &sync.RWMutex{},
		cache:         make(map[string][]byte, maxCapacity),
		ttls:          make(map[string]time.Time),
		quitChan:      make(chan struct{}),
	}

	go mc.deleteLoop(mc.quitChan)
	return mc
}

func (mc *MemCache) Stop() {
	close(mc.quitChan)
}

func (mc *MemCache) Set(key, value []byte, ttl uint32) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	mc.cache[string(key)] = value
	mc.ttls[string(key)] = time.Now().Add(time.Duration(ttl) * time.Second)

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
	return mc.delete(key)
}
func (mc *MemCache) delete(key []byte) error {
	if _, ok := mc.cache[string(key)]; !ok {
		return fmt.Errorf("[MemCache]: [%s] %w", string(key), ErrNotFound)
	}
	delete(mc.cache, string(key))
	delete(mc.ttls, string(key))
	return nil
}

func (mc *MemCache) deleteLoop(quitCh <-chan struct{}) {
	ticker := time.NewTicker(time.Duration(DeleteInterval) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			// delete
			mc.mtx.Lock()
			for key, ttl := range mc.ttls {
				if ttl.Before(time.Now()) {
					if err := mc.delete([]byte(key)); err != nil {
						fmt.Println("[MemCache]:", err)
					}
					fmt.Printf("[MemCache]: %s deleted\n", key)
				}
			}
			mc.mtx.Unlock()
		case <-quitCh:
			fmt.Println("[Memcache]: stopping delete loop...")
			return
		}
	}

}
