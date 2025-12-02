package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/skysparq/dispatcher"
)

type Source[T any] struct {
	client   *sqs.Client
	params   *sqs.ReceiveMessageInput
	messages []types.Message
}

func NewAwsSource[T any](queueUrl *string, maxMessages int32) *Source[T] {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Sprintf(`error creating AWS source: %v`, err))
	}
	return &Source[T]{
		client:   sqs.NewFromConfig(cfg),
		messages: make([]types.Message, 0, maxMessages),
		params: &sqs.ReceiveMessageInput{
			QueueUrl:            queueUrl,
			MaxNumberOfMessages: maxMessages,
			WaitTimeSeconds:     20,
		},
	}
}

func (a *Source[T]) Receive(ctx context.Context) ([]T, error) {
	a.messages = a.messages[:0]
	msg, err := a.client.ReceiveMessage(ctx, a.params)
	if err != nil {
		return nil, fmt.Errorf(`error receiving AWS messages: %w`, err)
	}
	result := make([]T, 0, len(a.messages))
	for _, message := range msg.Messages {
		if message.Body == nil {
			continue
		}
		var payload T
		err = json.Unmarshal([]byte(*message.Body), &payload)
		if err != nil {
			return nil, fmt.Errorf(`error receiving AWS message: error unmarshalling message: %w`, err)
		}
		a.messages = append(a.messages, message)
		result = append(result, payload)
	}
	return result, nil
}

func (a *Source[T]) ClearExcept(failures []dispatcher.RecordError) error {
	deletes := make([]types.DeleteMessageBatchRequestEntry, 0, len(a.messages))
	for i, message := range a.messages {
		if slices.ContainsFunc(failures, func(failure dispatcher.RecordError) bool { return failure.Index == i }) {
			continue
		}
		deletes = append(deletes, types.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})
	}
	_, err := a.client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
		Entries:  deletes,
		QueueUrl: a.params.QueueUrl,
	})
	if err != nil {
		return fmt.Errorf(`error clearing messages: %w`, err)
	}
	return nil
}
