package transport

import "github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"

type Peer struct {
	conn cachepb.CacheServiceClient
	addr string
}
