package main

import (
	"fmt"
	"log"

	"bigcat"
)

func main() {
	fmt.Println("🐾 Starting BigCat Storage Engine...")

	// Open DB
	db, err := bigcat.Open("./tmp_data")
	if err != nil {
		log.Fatalf("Failed to open BigCat: %v", err)
	}
	defer db.Close()

	// Put
	key := []byte("greeting")
	val := []byte("hello bigcat!")
	if err := db.Put(key, val); err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	// Get
	retrieved, err := db.Get(key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Retrieved: %s -> %s\n", string(key), string(retrieved))
}
