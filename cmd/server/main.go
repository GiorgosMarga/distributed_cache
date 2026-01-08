package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/GiorgosMarga/distributed_cache/internal/transport"
)

func gracefullShutdown(s *transport.Server, quitChan chan os.Signal) {
	<-quitChan
	fmt.Println("Shutting down")
	s.Stop()
}

func startServer(address string, connectWith string) error {
	quitChan := make(chan os.Signal, 1)

	signal.Notify(quitChan, syscall.SIGTERM, syscall.SIGINT)

	server := transport.NewServer(address)
	go func() {
		if connectWith == address {
			return
		}
		time.Sleep(1 * time.Second)
		if err := server.ConnectWith(connectWith); err != nil {
			fmt.Println(err)
		}
	}()
	go gracefullShutdown(server, quitChan)

	fmt.Println("here")
	return server.Start()

}

func main() {
	var (
		servers     int
		address     int
		connectWith string
	)
	flag.IntVar(&servers, "servers", 10, "number of servers")
	flag.StringVar(&connectWith, "connectWith", ":3000", "bootstrap node")
	flag.IntVar(&address, "address", 3000, "the first address of the server. If 10 servers are set then the address go from address to address + 10")
	flag.Parse()

	// quitChan := make(chan os.Signal)

	// signal.Notify(quitChan, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	for i := range servers {
		wg.Go(func() {
			startServer(fmt.Sprintf(":%d", address+i), connectWith)
		})
	}
	wg.Wait()

}
