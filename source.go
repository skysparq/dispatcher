package dispatcher

import (
	"context"
	"sync"
)

type Source[T any] interface {
	Receive(context.Context) ([]T, error)
	ClearExcept([]RecordError) error
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

func (a *ChanSource[T]) Receive(ctx context.Context) ([]T, error) {
	var result T
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result = <-a.message:
		return []T{result}, nil
	}
}

func (a *ChanSource[T]) ClearExcept(_ []RecordError) error {
	return nil
}
