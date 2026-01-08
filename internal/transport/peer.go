package transport

import (
	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"google.golang.org/grpc"
)

type Peer struct {
	conn   *grpc.ClientConn
	client cachepb.CacheServiceClient
	addr   string
}

func (p *Peer) Close() error {
	return p.conn.Close()
}
