package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/GiorgosMarga/distributed_cache/internal/transport"
)

func gracefullShutdown(s *transport.Server, quitChan chan os.Signal) {
	<-quitChan
	s.Stop()
}

func main() {
	var (
		// servers     int
		address        string
		bootstrapNodes []string
	)
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, syscall.SIGTERM, syscall.SIGINT)
	// flag.IntVar(&servers, "servers", 10, "number of servers")
	flag.Func("connectWith", "Bootstrap nodes to connect on Start().", func(s string) error {
		bootstrapNodes = strings.Split(s, ",")
		return nil
	})
	flag.StringVar(&address, "address", ":3000", "the first address of the server. If 10 servers are set then the address go from address to address + 10")
	flag.Parse()

	if !strings.Contains(address, ":") {
		address = ":" + address
	}
	server := transport.NewServer(address)

	go gracefullShutdown(server, quitChan)

	log.Fatal(server.Start(bootstrapNodes...))
}
