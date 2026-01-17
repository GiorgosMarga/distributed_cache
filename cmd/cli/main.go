package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/GiorgosMarga/distributed_cache/gen/go/cachepb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr string
	timeout    time.Duration
	conn       *grpc.ClientConn
	client     cachepb.CacheServiceClient
)
var rootCmd = &cobra.Command{
	Use:   "apispy [url]",
	Short: "This CLI client serves as the primary interface for interacting with the distributed caching cluster.",
	Long:  `This CLI provides a standard interface for performing Get, Set, and Delete operations against the cache cluster. It communicates via gRPC to any available cluster node, leveraging the cluster's internal request-forwarding and consistent hashing to manage data across multiple peers.`,
	Example: `  # Set a value with a 10s TTL
  cache-cli set --key mykey --val myval --ttl 10s

  # Get a value from the cache
  cache-cli get --key mykey`,
	Args: cobra.MaximumNArgs(1),
}
var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Retrieve a value from the cache",
	Args:  cobra.ExactArgs(1), // Ensures the user provides exactly one key
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		resp, err := client.Get(ctx, &cachepb.GetRequest{
			Key: []byte(key),
		})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%+v\n", resp)
	},
}
var setCmd = &cobra.Command{
	Use:   "set [key] [value] [ttl]",
	Short: "Set a key value pair with ttl second.",
	Args:  cobra.ExactArgs(3), // Ensures the user provides exactly one key
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		val := args[1]
		ttl, err := strconv.ParseUint(args[2], 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		resp, err := client.Set(ctx, &cachepb.SetRequest{
			Key:   []byte(key),
			Value: []byte(val),
			Ttl:   uint32(ttl),
		})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%+v\n", resp)
	},
}

func init() {
	// PersistentFlags are available to this command and all sub-commands
	rootCmd.PersistentFlags().StringVarP(&serverAddr, "addr", "a", "localhost:3000", "Address of the cache node")
	rootCmd.PersistentFlags().DurationVarP(&timeout, "timeout", "t", 5*time.Second, "Context timeout for the request")

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	client = cachepb.NewCacheServiceClient(conn)

	rootCmd.AddCommand(getCmd, setCmd)
}
func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
