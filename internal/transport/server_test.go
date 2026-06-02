package transport

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSet(t *testing.T) {

	for range 1_000_000 {
		conn, err := grpc.NewClient(":3000", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		client := cachepb.NewCacheServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := client.Set(ctx, &cachepb.SetRequest{
			Item: &cachepb.Item{
				Key:   fmt.Appendf(nil, "%d", rand.IntN(1_000_000)),
				Value: []byte("world"),
				Ttl:   100000,
			},
		}); err != nil {
			t.Fatal(err)
		}
		cancel()
		conn.Close()
	}
}

func freeAddress(tb testing.TB) string {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen on ephemeral port: %v", err)
	}
	defer ln.Close()

	return ln.Addr().String()
}

func startBenchmarkServer(tb testing.TB) (*Server, string) {
	tb.Helper()

	addr := freeAddress(tb)
	srv := NewServer(addr)

	go func() {
		if err := srv.Start(); err != nil {
			fmt.Printf("benchmark server stopped: %v\n", err)
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			tb.Fatalf("server did not start on %s: %v", addr, err)
		}
		time.Sleep(25 * time.Millisecond)
	}

	tb.Cleanup(srv.Stop)

	return srv, addr
}

func BenchmarkSetRPC(b *testing.B) {
	_, addr := startBenchmarkServer(b)
	b.ReportAllocs()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial server: %v", err)
	}
	b.Cleanup(func() { _ = conn.Close() })

	client := cachepb.NewCacheServiceClient(conn)
	ctx := context.Background()
	req := &cachepb.SetRequest{
		Item: &cachepb.Item{
			Key:   []byte("bench-key"),
			Value: []byte("bench-value"),
			Ttl:   60,
		},
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Set(ctx, req); err != nil {
			b.Fatalf("set request failed: %v", err)
		}
	}
}

func BenchmarkGetRPC(b *testing.B) {
	_, addr := startBenchmarkServer(b)
	b.ReportAllocs()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial server: %v", err)
	}
	b.Cleanup(func() { _ = conn.Close() })

	client := cachepb.NewCacheServiceClient(conn)
	ctx := context.Background()

	if _, err := client.Set(ctx, &cachepb.SetRequest{
		Item: &cachepb.Item{
			Key:   []byte("bench-key"),
			Value: []byte("bench-value"),
			Ttl:   60,
		},
	}); err != nil {
		b.Fatalf("seed key failed: %v", err)
	}

	req := &cachepb.GetRequest{Key: []byte("bench-key")}

	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Get(ctx, req); err != nil {
			b.Fatalf("get request failed: %v", err)
		}
	}
}
