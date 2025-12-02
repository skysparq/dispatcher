package dispatcher

import (
	"context"
	"sync"
)

type Source[T any] interface {
	Receive(context.Context) (T, error)
	Clear() error
}

type ChanSource[T any] struct {
	message chan T
	mutex   sync.Mutex
}

func NewChanSource[T any](messageChan chan T) *ChanSource[T] {
	source := &ChanSource[T]{
		message: messageChan,
	}
	return source
}

func (a *ChanSource[T]) Receive(ctx context.Context) (T, error) {
	var result T
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case result = <-a.message:
		return result, nil
	}
}

func (a *ChanSource[T]) Clear() error {
	return nil
}
