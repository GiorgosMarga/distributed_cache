package cluster

import (
	"fmt"
	"hash/crc32"
	"slices"
)

type HashRing struct {
	servers  map[uint32]string
	keys     []uint32
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		keys:     make([]uint32, 0),
		servers:  make(map[uint32]string, 0),
		replicas: replicas,
	}
}

func (hr *HashRing) AddServer(addr string) {
	for i := range hr.replicas {
		hash := crc32.ChecksumIEEE(fmt.Appendf(nil, "%s_%d", addr, i))
		hr.keys = append(hr.keys, hash)
		hr.servers[hash] = addr
	}

	slices.Sort(hr.keys)
}

func (hr *HashRing) GetAddrFromKey(k []byte) (string, error) {
	pos := crc32.ChecksumIEEE([]byte(k))

	idx, _ := slices.BinarySearch(hr.keys, pos)

	if idx == len(hr.keys) {
		idx = 0
	}
	return hr.servers[hr.keys[idx]], nil
}

func (hr *HashRing) Remove(serverAddr string) {
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
