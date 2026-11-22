package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bigcat"
	"bigcat/internal/server"
)

func main() {
	port := flag.Int("port", 6380, "Port to listen on (default 6380)")
	dataDir := flag.String("dir", "./data", "Directory to store BigCat data files")
	syncOnWrite := flag.Bool("sync", true, "fsync data to disk after every write operation")
	flag.Parse()

	banner := `
    /\_____/\
   /  o   o  \     🐾 BigCat Key-Value Storage Engine
  ( ==  ^  == )    ------------------------------------
   )         (     📡 Port:     %d
  (           )    📂 Data Dir: %s
 ( (  |   |  ) )   ⚡ Client:   redis-cli -p %d
(__(__)___(__)__)  ------------------------------------
`
	fmt.Printf(banner, *port, *dataDir, *port)

	// 1. Open BigCat DB
	opts := []bigcat.Option{
		bigcat.WithSyncOnWrite(*syncOnWrite),
	}
	db, err := bigcat.Open(*dataDir, opts...)
	if err != nil {
		log.Fatalf("❌ Failed to open BigCat database: %v", err)
	}

	// 2. Initialize and Start RESP TCP Server
	listenAddr := fmt.Sprintf("0.0.0.0:%d", *port)
	srv := server.NewServer(db, listenAddr)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	// 3. Graceful Shutdown on SIGINT / SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n🛑 Shutting down BigCat gracefully...")

	if err := srv.Stop(); err != nil {
		log.Printf("⚠️ Error during shutdown: %v", err)
	} else {
		fmt.Println("✅ BigCat shutdown complete. All data flushed to disk.")
	}
}
