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
	cache         map[string]*Data
	mtx           *sync.RWMutex
	quitChan      chan struct{}
}

func NewMemCache(maxCapacity uint32) *MemCache {
	mc := &MemCache{
		totalCapacity: 0,
		maxCapacity:   maxCapacity,
		mtx:           &sync.RWMutex{},
		cache:         make(map[string]*Data, maxCapacity),
		quitChan:      make(chan struct{}),
	}

	go mc.deleteLoop()
	return mc
}

func (mc *MemCache) Stop() {
	close(mc.quitChan)
}

func (mc *MemCache) Set(item *Data) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	if item.InsertedAt == 0 {
		item.InsertedAt = time.Now().Unix()
	}

	mc.cache[string(item.Key)] = item

	return nil
}

func (mc *MemCache) Get(key []byte) ([]byte, error) {
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()

	v, ok := mc.cache[string(key)]
	if !ok {
		return nil, fmt.Errorf("[MemCache]: [%s] %w", string(key), ErrNotFound)
	}

	return v.Value, nil
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
	return nil
}

func (mc *MemCache) deleteLoop() {
	ticker := time.NewTicker(time.Duration(DeleteInterval) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			// delete
			mc.mtx.Lock()
			for key, d := range mc.cache {

				if time.Now().Unix() > (d.InsertedAt + int64(d.Ttl)) {
					if err := mc.delete([]byte(key)); err != nil {
						fmt.Println("[MemCache]:", err)
					}
					fmt.Printf("[MemCache]: %s deleted\n", key)
				}
			}
			mc.mtx.Unlock()
		case <-mc.quitChan:
			fmt.Println("[Memcache]: stopping delete loop...")
			return
		}
	}

}

func (mc *MemCache) GetData() []*Data {
	idx := 0
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()
	items := make([]*Data, len(mc.cache))
	for key, item := range mc.cache {
		items[idx] = &Data{
			Key:        []byte(key),
			Value:      item.Value,
			Ttl:        item.Ttl,
			InsertedAt: item.InsertedAt,
		}
		idx++
	}
	return items
}
