package main

import (
	"log"
	"net/http"
	"os"

	"rtgdb/server"
	"rtgdb/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	kv := store.NewInMemoryStore(store.DefaultMaxValueSize)
	handler := server.NewHandler(kv, store.DefaultMaxValueSize)

	addr := ":" + port
	log.Printf("kv server listening on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
