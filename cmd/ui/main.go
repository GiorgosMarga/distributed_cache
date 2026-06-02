package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type uiServer struct {
	addr    string
	client  cachepb.CacheServiceClient
	conn    *grpc.ClientConn
	cluster *clusterManager
	mu      sync.Mutex
	snaps   map[string]*snapshotClient
}

type snapshotClient struct {
	conn   *grpc.ClientConn
	client cachepb.CacheServiceClient
}

type pageData struct {
	Backend   string
	Bootstrap string
}

type snapshotItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   uint32 `json:"ttl"`
}

type snapshotServer struct {
	Address string         `json:"address"`
	Items   []snapshotItem `json:"items"`
	Error   string         `json:"error,omitempty"`
}

type snapshotPayload struct {
	Servers []snapshotServer `json:"servers"`
}

func main() {
	var (
		backendAddr string
		listenAddr  string
		serverBin   string
	)

	flag.StringVar(&backendAddr, "backend", "", "gRPC address of the cache node")
	flag.StringVar(&listenAddr, "listen", ":8080", "HTTP address for the UI")
	flag.StringVar(&serverBin, "server-bin", "", "Path to the cache server binary")
	flag.Parse()

	if backendAddr == "" {
		if env := os.Getenv("CACHE_BACKEND"); env != "" {
			backendAddr = env
		} else {
			backendAddr = "localhost:5000"
		}
	}

	if serverBin == "" {
		serverBin = os.Getenv("CACHE_SERVER_BIN")
	}

	bootstrapNode := ":" + strconv.Itoa(portFromAddress(backendAddr))
	manager := newClusterManager(serverBin, bootstrapNode)
	if err := manager.ensureBootstrap(); err != nil {
		log.Fatal(err)
	}

	tpl := template.Must(template.ParseFiles("cmd/ui/templates/index.html"))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if err := tpl.ExecuteTemplate(w, "index.html", pageData{Backend: backendAddr, Bootstrap: bootstrapNode}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	ui, err := newUIServer(backendAddr, manager)
	if err != nil {
		log.Fatal(err)
	}
	defer ui.Close()

	fmt.Printf("Connected with cache server %s\n", backendAddr)
	http.HandleFunc("/api/get", ui.handleGet)
	http.HandleFunc("/api/set", ui.handleSet)
	http.HandleFunc("/api/delete", ui.handleDelete)
	http.HandleFunc("/api/cluster/snapshot", ui.handleClusterSnapshot)
	http.HandleFunc("/api/cluster/add", ui.handleClusterAdd)
	http.HandleFunc("/api/cluster/remove", ui.handleClusterRemove)
	http.HandleFunc("/api/cluster/reset", ui.handleClusterReset)

	log.Printf("UI listening on http://localhost%s and using backend %s", listenAddr, backendAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

func newUIServer(addr string, cluster *clusterManager) (*uiServer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &uiServer{
		addr:    addr,
		conn:    conn,
		client:  cachepb.NewCacheServiceClient(conn),
		cluster: cluster,
		snaps:   make(map[string]*snapshotClient),
	}, nil
}

func (s *uiServer) Close() error {
	s.mu.Lock()
	for addr, snap := range s.snaps {
		_ = snap.conn.Close()
		delete(s.snaps, addr)
	}
	s.mu.Unlock()
	return s.conn.Close()
}

func portFromAddress(addr string) int {
	raw := strings.TrimPrefix(addr, ":")
	raw = strings.TrimPrefix(raw, "localhost:")
	port, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return port
}

type keyRequest struct {
	Key string `json:"key"`
}

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   uint32 `json:"ttl"`
}

func (s *uiServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req keyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := s.client.Get(ctx, &cachepb.GetRequest{Key: []byte(req.Key)})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"hit":   resp.GetHit(),
		"value": string(resp.GetValue()),
		"key":   req.Key,
	})
}

func (s *uiServer) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := s.client.Set(ctx, &cachepb.SetRequest{
		Item: &cachepb.Item{
			Key:   []byte(req.Key),
			Value: []byte(req.Value),
			Ttl:   req.TTL,
		},
	})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": resp.GetSuccess(),
		"key":     req.Key,
		"ttl":     req.TTL,
	})
}

func (s *uiServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req keyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := s.client.Delete(ctx, &cachepb.DeleteRequest{Key: []byte(req.Key)})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": resp.GetSuccess(),
		"key":     req.Key,
	})
}

func (s *uiServer) handleClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	servers := s.cluster.getServers()
	results := make([]snapshotServer, 0, len(servers))
	for _, addr := range servers {
		items, err := s.fetchServerSnapshot(r.Context(), addr)
		if err != nil {
			if isGrpcNotReady(err) {
				results = append(results, snapshotServer{
					Address: addr,
					Items:   []snapshotItem{},
				})
				continue
			}

			results = append(results, snapshotServer{
				Address: addr,
				Error:   err.Error(),
			})
			continue
		}

		sort.Slice(items, func(i, j int) bool {
			return items[i].Key < items[j].Key
		})

		results = append(results, snapshotServer{
			Address: addr,
			Items:   items,
		})
	}

	writeJSON(w, http.StatusOK, snapshotPayload{Servers: results})
}

func (s *uiServer) handleClusterAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	addr, err := s.cluster.startNext()
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"address": addr,
	})
}

func (s *uiServer) handleClusterRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	addr, err := s.cluster.removeLast()
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"address": addr,
	})
}

func (s *uiServer) handleClusterReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.cluster.reset(); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"address": s.cluster.bootstrapAddr,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *uiServer) fetchServerSnapshot(parent context.Context, addr string) ([]snapshotItem, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	client, err := s.snapshotClient(addr)
	if err != nil {
		return nil, err
	}

	resp, err := client.GetAll(ctx, &cachepb.GetAllRequest{})
	if err != nil {
		return nil, err
	}

	items := make([]snapshotItem, 0, len(resp.GetItems()))
	for _, item := range resp.GetItems() {
		items = append(items, snapshotItem{
			Key:   string(item.GetKey()),
			Value: string(item.GetValue()),
			TTL:   item.GetTtl(),
		})
	}

	return items, nil
}

func isGrpcNotReady(err error) bool {
	if err == nil {
		return false
	}

	if status.Code(err) == codes.Unavailable {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "grpc not ready") || strings.Contains(msg, "not ready")
}

func (s *uiServer) snapshotClient(addr string) (cachepb.CacheServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snap, ok := s.snaps[addr]; ok {
		return snap.client, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	snap := &snapshotClient{
		conn:   conn,
		client: cachepb.NewCacheServiceClient(conn),
	}
	s.snaps[addr] = snap
	return snap.client, nil
}
