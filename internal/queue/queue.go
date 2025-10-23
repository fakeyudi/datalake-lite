package queue

import (
    "context"
    "errors"
    "log"
    "sync"
    "time"
)

type Queue interface {
    Enqueue(item string) error
    Dequeue(ctx context.Context) (string, error)
    Close() error
}

type InMemoryQueue struct {
    ch chan string
    wg sync.WaitGroup
}

func NewInMemoryQueue(buffer int) *InMemoryQueue {
    return &InMemoryQueue{ch: make(chan string, buffer)}
}

func (q *InMemoryQueue) Enqueue(item string) error {
    select {
    case q.ch <- item:
        return nil
    case <-time.After(5 * time.Second):
        return errors.New("enqueue timeout")
    }
}

func (q *InMemoryQueue) Dequeue(ctx context.Context) (string, error) {
    select {
    case item := <-q.ch:
        return item, nil
    case <-ctx.Done():
        return "", ctx.Err()
    }
}

func (q *InMemoryQueue) Close() error {
    close(q.ch)
    q.wg.Wait()
    return nil
}

// Placeholder for RabbitMQ-based queue
type RabbitMQQueue struct{}

func NewRabbitMQQueue(url string) (*RabbitMQQueue, error) {
    log.Printf("RabbitMQ queue not implemented yet (url=%s)", url)
    return &RabbitMQQueue{}, nil
}
