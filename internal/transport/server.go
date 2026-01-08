package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"github.com/GiorgosMarga/distributed_cache/internal/cache"
	"github.com/GiorgosMarga/distributed_cache/internal/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	cachepb.UnimplementedCacheServiceServer
	cache      cache.Cache
	addr       string
	grpcServer *grpc.Server
	// The Peer Map
	// Key:   Node Address (e.g., "192.168.1.10:50051")
	// Value: The gRPC client used to talk to that node
	peers    map[string]*Peer
	mu       *sync.RWMutex
	hashRing *cluster.HashRing
}

func NewServer(addr string) *Server {
	s := &Server{
		cache:    cache.NewMemCache(1024),
		addr:     addr,
		mu:       &sync.RWMutex{},
		peers:    make(map[string]*Peer),
		hashRing: cluster.NewHashRing(1024),
	}

	s.hashRing.AddServer(addr)
	return s
}

// Start starts the grpc server and blocks.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	cachepb.RegisterCacheServiceServer(s.grpcServer, s)

	fmt.Printf("[Server]: Is listening on address %s\n", s.addr)

	return s.grpcServer.Serve(ln)

}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

func (s *Server) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	key := req.GetKey()

	serverAddr, err := s.hashRing.GetAddrFromKey(key)
	if err != nil {
		return nil, err
	}

	if serverAddr != s.addr {
		peer, ok := s.peers[serverAddr]
		if !ok {
			return &cachepb.SetResponse{
				Success: false,
			}, nil
		}
		return peer.conn.Set(ctx, req)
	}
	fmt.Printf("[%s]: Setting %s -> %s\n", s.addr, key, req.GetValue())
	if err := s.cache.Set(req.GetKey(), req.GetValue(), req.GetTtl()); err != nil {
		return &cachepb.SetResponse{
			Success: false,
		}, nil
	}

	return &cachepb.SetResponse{
		Success: true,
	}, nil
}
func (s *Server) ConnectWith(peerAddr string) error {
	fmt.Printf("[%s]: Connecting with %s\n", s.addr, peerAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	newPeers := make([]string, 1)
	newPeers[0] = peerAddr
	totalPeers := 1

	for i := 0; i < totalPeers; i++ {
		peerAddr := newPeers[i]
		// already connected with peer
		s.mu.RLock()
		_, exists := s.peers[peerAddr]
		s.mu.RUnlock()

		if exists {
			continue
		}

		client, resp, err := s.connectWith(ctx, peerAddr)
		if err != nil {
			fmt.Printf("[%s]: Error connecting with %s, %s", s.addr, newPeers[i], err)
			continue
		}

		s.mu.Lock()
		s.peers[peerAddr] = &Peer{
			conn: client,
			addr: peerAddr,
		}
		s.mu.Unlock()
		newPeerAddresses := resp.GetPeerAddresses()
		newPeers = append(newPeers, newPeerAddresses...)
		totalPeers += len(newPeerAddresses)
	}

	return nil
}
func (s *Server) connectWith(ctx context.Context, peer string) (cachepb.CacheServiceClient, *cachepb.JoinResponse, error) {
	fmt.Printf("[%s]: Trying to connect with %s\n", s.addr, peer)
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s]: Error connecting with %s\n", s.addr, peer)
		return nil, nil, err
	}
	client := cachepb.NewCacheServiceClient(conn)
	resp, err := client.Join(ctx, &cachepb.JoinRequest{NodeAddress: s.addr, TargetAddress: peer})
	if err != nil {
		fmt.Printf("[%s]: Error connecting with %s\n", s.addr, peer)
		return nil, nil, err
	}
	fmt.Printf("[%s]: connected with %s\n", s.addr, peer)

	return client, resp, nil
}
func (s *Server) Join(ctx context.Context, req *cachepb.JoinRequest) (*cachepb.JoinResponse, error) {
	peerAddr := req.GetNodeAddress()
	fmt.Printf("[%s]: Received join from %s\n", s.addr, peerAddr)

	s.mu.Lock()
	_, exists := s.peers[peerAddr]
	if !exists {
		go s.ConnectWith(peerAddr)
	}

	peers := make([]string, 0, len(s.peers))
	for peer := range s.peers {
		if peer == peerAddr {
			continue
		}
		peers = append(peers, peer)
	}
	s.hashRing.AddServer(peerAddr)
	s.mu.Unlock()
	return &cachepb.JoinResponse{PeerAddresses: peers}, nil
}

func (s *Server) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	key := req.GetKey()

	serverAddr, err := s.hashRing.GetAddrFromKey(key)
	if err != nil {
		return nil, err
	}

	if serverAddr != s.addr {
		peer, ok := s.peers[serverAddr]
		if !ok {
			return nil, fmt.Errorf("peer is dead")
		}
		return peer.conn.Get(ctx, req)
	}

	fmt.Printf("[%s]: Getting %s\n", s.addr, key)

	v, err := s.cache.Get(key)
	if err != nil {
		return &cachepb.GetResponse{
			Value: nil,
			Hit:   false,
		}, nil
	}

	return &cachepb.GetResponse{
		Value: v,
		Hit:   true,
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	err := s.cache.Delete(req.GetKey())
	if err != nil {
		switch {
		case errors.Is(err, cache.ErrNotFound):
			return &cachepb.DeleteResponse{
				Success: false,
			}, nil
		default:
			return nil, err
		}
	}

	return &cachepb.DeleteResponse{
		Success: true,
	}, nil
}

func (s *Server) PrintPeers() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fmt.Printf("[%s]:", s.addr)
	for p := range s.peers {
		fmt.Printf("\t%s", p)
	}
	fmt.Println()
}
