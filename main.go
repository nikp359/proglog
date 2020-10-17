package main

import (
	"fmt"
	"time"
)

func main() {
	ch1 := make(chan int)

	for i := 0; i < 10; i++ {
		go func(idx int) {
			select {
			case ch1 <- idx:
				fmt.Println("Write to ch1")
			default:
				fmt.Println("skip")
			}

			ch1 <- idx
		}(i)
	}

	input1 := <-ch1
	input2 := <-ch1

	fmt.Println(input1)
	fmt.Println(input2)

	// srv := server.NewHTTPServer(":8080")
	// log.Fatal(srv.ListenAndServe())
	time.Sleep(2 * time.Second)
}
