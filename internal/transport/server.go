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

// TODO: debug why grpc call fails

type ServerOpts struct {
}

type Server struct {
	cachepb.UnimplementedCacheServiceServer
	cache      cache.Cache
	addr       string
	grpcServer *grpc.Server
	peers      map[string]*Peer
	mu         *sync.RWMutex
	hashRing   *cluster.HashRing
	ctx        context.Context
	cancelFunc context.CancelFunc
	ServerOpts
}

func NewServer(addr string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cache:      cache.NewMemCache(1024),
		addr:       addr,
		mu:         &sync.RWMutex{},
		peers:      make(map[string]*Peer),
		hashRing:   cluster.NewHashRing(1024),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	s.hashRing.AddServer(addr)
	return s
}

// Start starts the grpc server and blocks.
func (s *Server) Start(bootstrapNodes ...string) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	cachepb.RegisterCacheServiceServer(s.grpcServer, s)

	fmt.Printf("[Server]: Is listening on address %s\n", s.addr)
	go s.heartbeatLoop()
	go func() {
		for _, bootstrapNode := range bootstrapNodes {
			if err := s.ConnectWith(bootstrapNode); err != nil {
				fmt.Println(err)
			}
		}
	}()
	s.grpcServer.Serve(ln)
	return nil
}

// Stop stops the server, notifies the other peers and re-sets the keys on next server
func (s *Server) Stop() {
	fmt.Printf("[%s]: stopping...\n", s.addr)

	// stop the hearbeatloop
	defer s.cancelFunc()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s.mu.RLock()
	for _, peer := range s.peers {
		if _, err := peer.client.Leave(ctx, &cachepb.LeaveRequest{NodeAddress: s.addr}); err != nil {
			fmt.Printf("[%s]: Error sending leave message %s %s\n", s.addr, peer.addr, err)
		}
	}
	s.mu.RUnlock()

	s.hashRing.Remove(s.addr)

	if err := s.rebalance(); err != nil {
		fmt.Printf("[%s]: Error during rebalance: %s\n", s.addr, err)
	}

	for _, peer := range s.peers {
		if err := peer.Close(); err != nil {
			fmt.Printf("[%s]: Error closing %s\n", s.addr, peer.addr)
		}
	}

	s.cache.Stop()
	s.grpcServer.GracefulStop()
	fmt.Printf("[%s]: Stopped server\n", s.addr)
}

func (s *Server) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {

	key := req.GetItem().GetKey()

	serverAddr, err := s.hashRing.GetAddrFromKey(key)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(key), serverAddr, serverAddr)
	if serverAddr != s.addr {
		peer, ok := s.peers[serverAddr]
		if !ok {
			return &cachepb.SetResponse{
				Success: false,
			}, nil
		}
		return peer.client.Set(ctx, req)
	}
	fmt.Printf("[%s]: Setting %s -> %s\n", s.addr, key, req.Item.Value)
	if err := s.cache.Set(&cache.Data{
		Key:        key,
		Value:      req.Item.Value,
		Ttl:        req.Item.Ttl,
		InsertedAt: req.Item.InsertedAt,
	}); err != nil {
		return &cachepb.SetResponse{
			Success: false,
		}, nil
	}

	return &cachepb.SetResponse{
		Success: true,
	}, nil
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(1) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			peers := make([]*Peer, 0, len(s.peers))
			for _, peer := range s.peers {
				peers = append(peers, peer)
			}
			s.mu.Unlock()
			for _, peer := range peers {
				if _, err := peer.client.IsAlive(s.ctx, &cachepb.AliveRequest{NodeAddress: s.addr}); err != nil {
					s.hashRing.Remove(peer.addr)
					s.mu.Lock()
					delete(s.peers, peer.addr)
					s.mu.Unlock()
				}
			}
		case <-s.ctx.Done():
			fmt.Printf("[%s]: stopping heartbeat loop\n", s.addr)
			return
		}
	}
}
func (s *Server) ConnectWith(peerAddr string) error {
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

		peer, resp, err := s.connectWith(ctx, peerAddr)
		if err != nil {
			fmt.Printf("[%s]: Error connecting with %s, %s", s.addr, newPeers[i], err)
			continue
		}

		s.mu.Lock()
		s.peers[peerAddr] = peer
		s.mu.Unlock()
		fmt.Printf("[%s]: Connected with %s\n", s.addr, peerAddr)
		newPeerAddresses := resp.GetPeerAddresses()
		newPeers = append(newPeers, newPeerAddresses...)
		totalPeers += len(newPeerAddresses)
	}

	return nil
}
func (s *Server) connectWith(ctx context.Context, peer string) (*Peer, *cachepb.JoinResponse, error) {
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
	s.hashRing.AddServer(peer)

	newPeer := &Peer{client: client, conn: conn, addr: peer}
	s.mu.Lock()
	s.peers[peer] = newPeer
	s.mu.Unlock()
	// rebalance
	if err := s.rebalance(); err != nil {
		fmt.Printf("[%s]: Error during rebalance: %s\n", s.addr, err)
	}

	return newPeer, resp, nil
}
func (s *Server) Join(ctx context.Context, req *cachepb.JoinRequest) (*cachepb.JoinResponse, error) {
	peerAddr := req.GetNodeAddress()
	s.mu.Lock()
	defer s.mu.Unlock()

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
	return &cachepb.JoinResponse{PeerAddresses: peers}, nil
}
func (s *Server) TransferKeys(ctx context.Context, req *cachepb.TransferRequest) (*cachepb.TransferResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var nkeys int32
	for _, item := range req.GetItems() {
		fmt.Printf("[%s]: Setting %q->%q\n", s.addr, item.Key, item.Value)
		if err := s.cache.Set(&cache.Data{
			Key:        item.Key,
			Value:      item.Value,
			Ttl:        item.Ttl,
			InsertedAt: item.InsertedAt,
		}); err != nil {
			return nil, err
		}
		nkeys++
	}

	return &cachepb.TransferResponse{Success: true, Nkeys: nkeys}, nil
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
		return peer.client.Get(ctx, req)
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

func (s *Server) IsAlive(ctx context.Context, req *cachepb.AliveRequest) (*cachepb.AliveResponse, error) {
	from := req.GetNodeAddress()
	s.mu.RLock()
	_, exists := s.peers[from]
	s.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("peer was not found")
	}
	return &cachepb.AliveResponse{
		Success: true,
	}, nil
}

func (s *Server) Leave(ctx context.Context, req *cachepb.LeaveRequest) (*cachepb.LeaveResponse, error) {
	from := req.GetNodeAddress()
	s.mu.RLock()
	peer, exists := s.peers[from]
	s.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("peer was not found")
	}
	fmt.Printf("[%s]: Removing %s...\n", s.addr, from)
	s.hashRing.Remove(req.GetNodeAddress())
	if err := peer.conn.Close(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	delete(s.peers, from)
	s.mu.Unlock()
	return &cachepb.LeaveResponse{
		Success: true,
	}, nil
}
func (s *Server) GetAll(ctx context.Context, req *cachepb.GetAllRequest) (*cachepb.GetAllResponse, error) {
	data := s.cache.GetData()
	items := make([]*cachepb.Item, len(data))
	for idx, item := range data {
		items[idx] = &cachepb.Item{
			Key:   item.Key,
			Value: item.Value,
			Ttl:   item.Ttl,
		}
	}
	return &cachepb.GetAllResponse{Items: items}, nil
}

func (s *Server) rebalance() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := s.cache.GetData()
	keysToTransfer := make(map[string][]*cachepb.Item)
	for _, item := range data {
		addr, err := s.hashRing.GetAddrFromKey(item.Key)
		if err != nil {
			return err
		}
		if addr != s.addr {
			if _, exists := keysToTransfer[addr]; !exists {
				keysToTransfer[addr] = make([]*cachepb.Item, 0)
			}
			keysToTransfer[addr] = append(keysToTransfer[addr], &cachepb.Item{
				Key:        item.Key,
				Value:      item.Value,
				Ttl:        item.Ttl,
				InsertedAt: item.InsertedAt,
			})
		}
	}
	for addr, items := range keysToTransfer {
		peer, exists := s.peers[addr]
		if !exists {
			fmt.Printf("[%s]: Peer doesnt exists during rebalancing %q\n", s.addr, addr)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		if _, err := peer.client.TransferKeys(ctx, &cachepb.TransferRequest{
			Items: items,
		}); err != nil {
			fmt.Printf("[%s]: gRPC error while transfering keys to %q: %s\n", s.addr, addr, err)
			cancel()
			continue
		}
		cancel()
		//TODO: what happens to the rest of the keys if error occurs in delete
		for _, item := range items {
			if err := s.cache.Delete(item.Key); err != nil {
				fmt.Printf("[%s]: Error deleting key %q during rebalancing: %s\n", s.addr, addr, err)
				break
			}
		}
	}
	return nil
}
