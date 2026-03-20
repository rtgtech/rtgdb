package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rtgdb/server"
	"rtgdb/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	walPath := os.Getenv("WAL_PATH")
	if walPath == "" {
		walPath = "data/kv.wal"
	}

	syncMode := store.SyncMode(os.Getenv("WAL_SYNC_MODE"))
	if syncMode == "" {
		syncMode = store.SyncAlways
	}

	kv, err := store.NewStore(store.Config{
		MaxValueSize: store.DefaultMaxValueSize,
		WALPath:      walPath,
		SyncMode:     syncMode,
	})
	if err != nil {
		log.Fatalf("store initialization failed: %v", err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			log.Printf("store close failed: %v", err)
		}
	}()

	handler := server.NewHandler(kv, store.DefaultMaxValueSize)

	addr := ":" + port
	httpServer := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	go func() {
		<-stop

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("server shutdown failed: %v", err)
		}
	}()

	log.Printf("kv server listening on %s (wal=%s sync=%s)", addr, walPath, syncMode)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}
