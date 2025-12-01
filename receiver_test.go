package receiver_test

import (
	"context"
	"sync"
	"testing"
	"time"

	dispatcher "github.com/skysparq/dispatcher"
)

type TestProcessor struct {
	messages []string
	msgChan  chan TestPayload
	stop     chan bool
	mutex    sync.Mutex
}

func NewTestProcessor() *TestProcessor {
	processor := &TestProcessor{
		messages: make([]string, 0, 10),
		msgChan:  make(chan TestPayload),
		stop:     make(chan bool),
	}
	go processor.receiveLoop()
	return processor
}

func (t *TestProcessor) Incoming() chan TestPayload {
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
		msg, ok := <-t.msgChan
		if !ok {
			close(t.stop)
			return
		}
		t.mutex.Lock()
		t.messages = append(t.messages, msg.Message)
		t.mutex.Unlock()
	}
}

type TestPayload struct {
	Buck    string
	Message string
}

func (t TestPayload) Bucket() string {
	return t.Buck
}

func TestReceiver(t *testing.T) {
	bucket1 := NewTestProcessor()
	bucket2 := NewTestProcessor()
	incoming := make(chan TestPayload)
	source := dispatcher.NewChanSource(incoming)
	processors := map[string]dispatcher.Processor[TestPayload]{
		`bucket1`: bucket1,
		`bucket2`: bucket2,
	}
	r := dispatcher.NewReceiver(source, processors)

	ctx, cancel := context.WithCancel(context.Background())
	go r.Start(ctx)

	incoming <- TestPayload{
		Buck:    `bucket1`,
		Message: `hello bucket1`,
	}
	incoming <- TestPayload{
		Buck:    `bucket2`,
		Message: `hello bucket2`,
	}
	incoming <- TestPayload{
		Buck:    `bucket1`,
		Message: `hello again bucket1`,
	}

	time.Sleep(2000 * time.Millisecond)
	cancel()
	r.WaitForStop()

	if size := len(bucket1.Messages()); size != 2 {
		t.Fatalf(`expected 2 messages in bucket1 but got %v`, size)
	}

	if size := len(bucket2.Messages()); size != 1 {
		t.Fatalf(`expected 1 messages in bucket2 but got %v`, size)
	}
}
