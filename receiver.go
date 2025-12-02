package dispatcher

import (
	"context"
	"time"
)

type Logger interface {
	Errorf(format string, args ...interface{})
}

type Receiver[T any] struct {
	workers []*Worker[T]
	stopped []chan bool
	logger  Logger
}

type Worker[T any] struct {
	Source    Source[T]
	Processor Processor[T]
}

func NewReceiver[T any](workers []*Worker[T], logger Logger) *Receiver[T] {
	stopped := make([]chan bool, 0, len(workers))
	for range workers {
		stopped = append(stopped, make(chan bool))
	}
	return &Receiver[T]{
		workers: workers,
		stopped: stopped,
		logger:  logger,
	}
}

func (f *Receiver[T]) Start(ctx context.Context) {
	for i, worker := range f.workers {
		go f.work(ctx, worker.Source, worker.Processor, f.stopped[i])
	}
}

func (f *Receiver[T]) WaitForStop() {
	for _, stop := range f.stopped {
		<-stop
	}
}

func (f *Receiver[T]) work(ctx context.Context, source Source[T], processor Processor[T], stopped chan bool) {
	for {
		if ctx.Err() != nil {
			break
		}
		messages, err := source.Receive(ctx)
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

		messageErrs, err := processor.Process(messages)
		if err != nil {
			f.logger.Errorf(`error processing messages: %v`, err)
			continue
		}
		for _, msgErr := range messageErrs {
			f.logger.Errorf(`error processing message %v: %v`, msgErr.Index, msgErr.Error)
		}
		err = source.ClearExcept(messageErrs)
		if err != nil {
			f.logger.Errorf(`error clearing messages: %v`, err)
		}
	}
	close(stopped)
}
