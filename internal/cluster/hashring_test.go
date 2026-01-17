package cluster

import (
	"testing"
)

func TestAddServer(t *testing.T) {
	replicas := 10
	hr := NewHashRing(replicas)

	hr.AddServer(":3000")

	if len(hr.keys) != replicas {
		t.Errorf("wrong amount of keys. Expected: %d, got %d\n", replicas, len(hr.keys))
	}

	hr.AddServer(":3001")

	if len(hr.keys) != 2*replicas {
		t.Errorf("wrong amount of keys. Expected: %d, got %d\n", 2*replicas, len(hr.keys))
	}
}
