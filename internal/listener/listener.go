package listener

import (
    "context"
    "log"
    "path/filepath"

    "github.com/fsnotify/fsnotify"
    "github.com/fakeyudi/datalake-lite/internal/queue"
)

// WatchAndServe starts watching watchDir and enqueues new files to q.
func WatchAndServe(ctx context.Context, watchDir string, q queue.Queue) error {
    w, err := fsnotify.NewWatcher()
    if err != nil {
        return err
    }
    defer w.Close()

    if err := w.Add(watchDir); err != nil {
        return err
    }
    log.Printf("watching directory: %s", watchDir)

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case ev, ok := <-w.Events:
            if !ok {
                return nil
            }
            if ev.Op&fsnotify.Create == fsnotify.Create || ev.Op&fsnotify.Rename == fsnotify.Rename {
                abs, _ := filepath.Abs(ev.Name)
                log.Printf("enqueue file event: %s", abs)
                if err := q.Enqueue(abs); err != nil {
                    log.Printf("enqueue error: %v", err)
                }
            }
        case err, ok := <-w.Errors:
            if !ok {
                return nil
            }
            log.Printf("watcher error: %v", err)
        }
    }
}
