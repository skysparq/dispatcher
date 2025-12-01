package receiver

import (
	"context"
	"time"
)

type Logger interface {
	Errorf(format string, args ...interface{})
}

type Payload[T int | string] interface {
	Bucket() T
}

type Receiver[T Payload[K], K int | string] struct {
	source     Source[T]
	processors map[K]Processor[T]
	logger     Logger
	stop       chan bool
}

func NewReceiver[T Payload[K], K int | string](source Source[T], processors map[K]Processor[T]) *Receiver[T, K] {
	return &Receiver[T, K]{
		source:     source,
		processors: processors,
		stop:       make(chan bool),
	}
}

func (f *Receiver[T, K]) Start(ctx context.Context) {
	for {
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
		for _, message := range messages {
			bucket := message.Bucket()
			processor, ok := f.processors[bucket]
			if ok {
				processor.Incoming() <- message
			}
		}
	}
	for _, processor := range f.processors {
		processor.Close()
	}
}

func (f *Receiver[T, K]) WaitForStop() {
	for _, processor := range f.processors {
		processor.WaitForStop()
	}
}
