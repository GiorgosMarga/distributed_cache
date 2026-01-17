package cluster

import (
	"fmt"
	"hash/crc32"
	"slices"
	"sync"
)

type HashRing struct {
	servers  map[uint32]string
	keys     []uint32
	replicas int
	mu       *sync.Mutex
}

// NewHashRing returns a new HashRing.
// Each server takes 'replicas' positions in the ring (virtual nodes).
func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		keys:     make([]uint32, 0),
		servers:  make(map[uint32]string, 0),
		replicas: replicas,
		mu:       &sync.Mutex{},
	}
}

// AddServer create 'replicas' virtual server hashes and puts them in the ring.
func (hr *HashRing) AddServer(addr string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	for i := range hr.replicas {
		hash := crc32.ChecksumIEEE(fmt.Appendf(nil, "%s_%d", addr, i))
		hr.keys = append(hr.keys, hash)
		hr.servers[hash] = addr
	}

	slices.Sort(hr.keys)
}

// GetAddrFromKey hashes the key and finds the server it belongs to.
func (hr *HashRing) GetAddrFromKey(k []byte) (string, error) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	pos := crc32.ChecksumIEEE([]byte(k))

	idx, _ := slices.BinarySearch(hr.keys, pos)

	if idx == len(hr.keys) {
		// wrap around
		idx = 0
	}
	return hr.servers[hr.keys[idx]], nil
}

// Remove removes the given server address and the replicas from the ring.
func (hr *HashRing) Remove(serverAddr string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	keys := make([]uint32, 0, len(hr.keys)-hr.replicas)
	for _, key := range hr.keys {
		if hr.servers[key] == serverAddr {
			delete(hr.servers, key)
			continue
		}
		keys = append(keys, key)
	}
	// No need to sort again since we iterate over a sorted slice of keys (hashes)
	hr.keys = keys
}
