package dispatcher_test

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"github.com/skysparq/dispatcher"
)

type TestProcessor struct {
	Messages []string
}

func NewTestProcessor() *TestProcessor {
	processor := &TestProcessor{
		Messages: make([]string, 0, 10),
	}
	return processor
}

func (t *TestProcessor) Process(msgs []string) ([]dispatcher.RecordError, error) {
	for _, msg := range msgs {
		t.Messages = append(t.Messages, msg)
	}
	return nil, nil
}

func TestReceiver(t *testing.T) {
	processor := NewTestProcessor()
	incoming := make(chan string)
	source := dispatcher.NewChanSource[string](incoming)
	worker := &dispatcher.Worker[string]{
		Source:    source,
		Processor: processor,
	}
	r := dispatcher.NewReceiver([]*dispatcher.Worker[string]{worker}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	go r.Start(ctx)

	incoming <- `hello 1`
	incoming <- `hello 2`
	incoming <- `hello 3`

	cancel()
	r.WaitForStop()

	expected := []string{`hello 1`, `hello 2`, `hello 3`}
	if !slices.Equal(processor.Messages, expected) {
		t.Fatalf(`expected %v but got %v`, expected, processor.Messages)
	}
}
