package dispatcher_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/skysparq/dispatcher"
)

type TestProcessor struct {
	messages []string
	msgChan  chan []string
	stop     chan bool
	mutex    sync.Mutex
}

func NewTestProcessor() *TestProcessor {
	processor := &TestProcessor{
		messages: make([]string, 0, 10),
		msgChan:  make(chan []string),
		stop:     make(chan bool),
	}
	go processor.receiveLoop()
	return processor
}

func (t *TestProcessor) Incoming() chan []string {
	return t.msgChan
}

func (t *TestProcessor) Close() {
	close(t.msgChan)
}

func (t *TestProcessor) WaitForStop() {
	<-t.stop
}

func (t *TestProcessor) Messages() []string {
	t.mutex.Lock()
	messages := make([]string, len(t.messages))
	copy(messages, t.messages)
	t.mutex.Unlock()
	return messages
}

func (t *TestProcessor) receiveLoop() {
	for {
		messages, ok := <-t.msgChan
		if !ok {
			close(t.stop)
			return
		}
		t.mutex.Lock()
		t.messages = append(t.messages, messages...)

		t.mutex.Unlock()
	}
}

func TestReceiver(t *testing.T) {
	processor := NewTestProcessor()
	incoming := make(chan []string)
	source := dispatcher.NewChanSource(incoming)
	r := dispatcher.NewReceiver(source, processor, &dispatcher.StandardLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	go r.Start(ctx)

	incoming <- []string{`hello 1`}
	incoming <- []string{
		`hello 2`,
		`hello 3`,
	}

	time.Sleep(2000 * time.Millisecond)
	cancel()
	r.WaitForStop()

	expected := []string{`hello 1`, `hello 2`, `hello 3`}
	if !slices.Equal(processor.Messages(), expected) {
		t.Fatalf(`expected %v but got %v`, expected, processor.Messages())
	}
}
