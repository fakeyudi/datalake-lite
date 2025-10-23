package main

import (
    "context"
    "flag"
    "log"
    "os"
    "os/signal"
    "path/filepath"
    "syscall"
    "time"

    "github.com/fakeyudi/datalake-lite/internal/catalog"
    "github.com/fakeyudi/datalake-lite/internal/ingest"
    "github.com/fakeyudi/datalake-lite/internal/listener"
    "github.com/fakeyudi/datalake-lite/internal/queue"
    "github.com/fakeyudi/datalake-lite/internal/storage"
)

func main() {
    watchDir := flag.String("watch", "./data/incoming", "directory to watch for new files")
    dbPath := flag.String("db", "dl_catalog.db", "path to sqlite catalog")
    storageDir := flag.String("storage", "./data", "local storage path")
    buffer := flag.Int("buffer", 100, "queue buffer size")
    flag.Parse()

    absWatch, _ := filepath.Abs(*watchDir)

    cat, err := catalog.New(*dbPath)
    if err != nil {
        log.Fatalf("catalog init: %v", err)
    }
    defer cat.Close()

    st := storage.NewLocal(*storageDir)
    ing := ingest.New(st, cat)
    q := queue.NewInMemoryQueue(*buffer)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Worker
    go func() {
        for {
            item, err := q.Dequeue(ctx)
            if err != nil {
                log.Printf("queue error: %v", err)
                return
            }
            log.Printf("worker: ingesting %s", item)
            _, _, err = ing.IngestFile(item, "")
            if err != nil {
                log.Printf("ingest failed for %s: %v", item, err)
            } else {
                log.Printf("ingested %s", item)
            }
        }
    }()

    // Watcher
    go func() {
        if err := listener.WatchAndServe(ctx, absWatch, q); err != nil {
            log.Printf("watcher stopped: %v", err)
            cancel()
        }
    }()

    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    <-sigs
    log.Println("shutting down...")
    cancel()
    time.Sleep(500 * time.Millisecond)
    q.Close()
}
