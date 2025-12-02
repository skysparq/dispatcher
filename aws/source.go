package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Source[T any] struct {
	client  *sqs.Client
	params  *sqs.ReceiveMessageInput
	message types.Message
}

func NewAwsSource[T any](queueUrl *string) *Source[T] {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Sprintf(`error creating AWS source: %v`, err))
	}
	return &Source[T]{
		client: sqs.NewFromConfig(cfg),
		params: &sqs.ReceiveMessageInput{
			QueueUrl:            queueUrl,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
		},
	}
}

func (a *Source[T]) Receive(ctx context.Context) (T, error) {
	var result T
	msg, err := a.client.ReceiveMessage(ctx, a.params)
	if err != nil {
		return result, fmt.Errorf(`error receiving AWS messages: %w`, err)
	}
	a.message = msg.Messages[0]
	result, err = decodeMessageBody[T](a.message.Body)
	return result, nil
}

func (a *Source[T]) Clear() error {
	_, err := a.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		ReceiptHandle: a.message.ReceiptHandle,
		QueueUrl:      a.params.QueueUrl,
	})
	if err != nil {
		return fmt.Errorf(`error clearing messages: %w`, err)
	}
	return nil
}

func decodeMessageBody[T any](body *string) (T, error) {
	var payload T
	if body == nil {
		return payload, fmt.Errorf(`body is nil`)
	}
	err := json.Unmarshal([]byte(*body), &payload)
	if err != nil {
		return payload, fmt.Errorf(`error unmarshalling message: %w`, err)
	}
	return payload, nil
}
