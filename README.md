# shm #

A hash map implemented in a shared memory mapping.

Example:
```go
package main

import (
	"github.com/fengyoulin/shm"
	"log"
	"time"
)

func main() {
	m, err := shm.Create("map.db", 4096, 40, 32, 20, time.Second)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		err = m.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	// get or add a key
	b, err := m.Get("1a2b3c4d5e6f", true)
	if err != nil {
		log.Fatalln(err)
	}

	// do something with b
	log.Println(cap(b))

	// iterate over the map
	m.Foreach(func(key string, value []byte) bool {
		log.Printf("key: %s\n", key)
		return true
	})
	// m.Delete("key")
}
```
