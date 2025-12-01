package dispatcher

import (
	"context"
	"time"
)

type Logger interface {
	Errorf(format string, args ...interface{})
}

type Receiver[T any] struct {
	source    Source[T]
	processor Processor[T]
	logger    Logger
	stop      chan bool
}

func NewReceiver[T any](source Source[T], processor Processor[T], logger Logger) *Receiver[T] {
	return &Receiver[T]{
		source:    source,
		processor: processor,
		logger:    logger,
		stop:      make(chan bool),
	}
}

func (f *Receiver[T]) Start(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			break
		}
		messages, err := f.source.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			f.logger.Errorf(`error receiving messages: %v`, err)
			time.Sleep(10 * time.Second)
			continue
		}
		if len(messages) == 0 {
			continue
		}
		f.processor.Incoming() <- messages
	}
	f.processor.Close()
}

func (f *Receiver[T]) WaitForStop() {
	f.processor.WaitForStop()
}
