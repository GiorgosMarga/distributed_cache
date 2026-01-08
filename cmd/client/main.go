package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		clients           int
		messagesPerClient int
	)
	flag.IntVar(&clients, "totalClients", 1, "The number of total clients that will be created")
	flag.IntVar(&messagesPerClient, "messagesPerClient", 100, "The number of read/get a client will perform")
	flag.Parse()

	// wg := &sync.WaitGroup{}

	// for range clients {
	// 	wg.Go(func() {
	// 		serverAddr := 3000 + rand.Intn(3)
	// 		conn, err := grpc.NewClient(fmt.Sprintf(":%d", serverAddr), grpc.WithTransportCredentials(insecure.NewCredentials()))
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}

	// 		client := cachepb.NewCacheServiceClient(conn)
	// 		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	// 		defer cancel()
	// 		for i := range messagesPerClient {
	// 			if rand.Intn(10) < 5 {
	// 				resp, err := client.Set(ctx, &cachepb.SetRequest{
	// 					Key:   fmt.Appendf(nil, "%s_%d", "foo", i),
	// 					Value: []byte("bar"),
	// 					Ttl:   50,
	// 				})
	// 				if err != nil {
	// 					fmt.Println(err)
	// 				}
	// 				fmt.Printf("[%d]: Set Resp: %+v\n", serverAddr, resp)
	// 			} else {
	// 				resp, err := client.Get(ctx, &cachepb.GetRequest{
	// 					Key: fmt.Appendf(nil, "%s_%d", "foo", i),
	// 				})
	// 				if err != nil {
	// 					continue
	// 				}
	// 				fmt.Printf("[%d]: Key: foo_%d | Hit: %v | Value: %s\n", serverAddr, i, resp.Hit, string(resp.Value))
	// 			}
	// 		}
	// 	})
	// }

	// wg.Wait()

	conn, err := grpc.NewClient(fmt.Sprintf(":%d", 3000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	client := cachepb.NewCacheServiceClient(conn)
	// ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	// defer cancel()

	for i := range 100 {
		key := fmt.Appendf(nil, "key_%d", i)
		_, err := client.Set(context.Background(), &cachepb.SetRequest{Key: key, Value: []byte("test"), Ttl: 1000})
		if err != nil {
			fmt.Println("err:", err)
		}
	}

	fmt.Println("You have 1 minute")
	time.Sleep(1 * time.Minute)
	fmt.Println("You have 3 more seconds")
	time.Sleep(3 * time.Second)

	for i := range 100 {
		key := fmt.Appendf(nil, "key_%d", i)
		resp, err := client.Get(context.Background(), &cachepb.GetRequest{Key: key})
		if err != nil {
			fmt.Println("err:", err)
		}
		if resp.Hit == false {
			fmt.Println("Missed value:", string(key))
		}
	}

}
