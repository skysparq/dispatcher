package dispatcher

import (
	"context"
	"sync"
	"time"
)

type Source[T any] interface {
	Receive(context.Context) ([]T, error)
}

type ChanSource[T any] struct {
	messages []T
	mutex    sync.Mutex
}

func NewChanSource[T any](messageChan chan []T) *ChanSource[T] {
	source := &ChanSource[T]{
		messages: make([]T, 0, 100),
	}
	go source.start(messageChan)
	return source
}

func (a *ChanSource[T]) Receive(ctx context.Context) ([]T, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a.mutex.Lock()
		if len(a.messages) > 0 {
			break
		}
		a.mutex.Unlock()
		time.Sleep(time.Second)
	}
	result := make([]T, len(a.messages))
	copy(result, a.messages)
	a.messages = a.messages[:0]
	a.mutex.Unlock()
	return result, nil
}

func (a *ChanSource[T]) start(messageChan chan []T) {
	for message := range messageChan {
		a.mutex.Lock()
		a.messages = append(a.messages, message...)
		a.mutex.Unlock()
	}
}
