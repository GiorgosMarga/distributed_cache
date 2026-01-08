package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/GiorgosMarga/distributed_cache/internal/transport"
)

func main() {
	var servers int
	flag.IntVar(&servers, "servers", 10, "number of servers")
	flag.Parse()

	wg := &sync.WaitGroup{}

	for i := range servers {
		wg.Go(func() {
			server := transport.NewServer(fmt.Sprintf(":%d", 3000+i))
			go func(i int) {
				if i == 0 {
					return
				}
				time.Sleep(1 * time.Second)
				if err := server.ConnectWith(":3000"); err != nil {
					fmt.Println(err)
				}

			}(i)
			if err := server.Start(); err != nil {
				fmt.Println(err)
			}
		})
		time.Sleep(1 * time.Second)

	}

	wg.Wait()

}
