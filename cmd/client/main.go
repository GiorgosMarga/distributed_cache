package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(":3000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	client := cachepb.NewCacheServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := range 1000 {
		if rand.Intn(10) < 5 {
			resp, err := client.Set(ctx, &cachepb.SetRequest{
				Key:   fmt.Appendf(nil, "%s_%d", "foo", i%100),
				Value: []byte("bar"),
				Ttl:   50,
			})
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("Set Resp: %+v\n", resp)
		} else {
			resp, err := client.Get(ctx, &cachepb.GetRequest{
				Key: fmt.Appendf(nil, "%s_%d", "foo", i%10),
			})
			if err != nil {
				continue
			}
			fmt.Printf("Key: foo_%d | Hit: %v | Value: %s\n", i, resp.Hit, string(resp.Value))
		}
	}

}
