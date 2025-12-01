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
	client *sqs.Client
	params *sqs.ReceiveMessageInput
}

func NewAwsSource[T any](queueUrl *string, maxMessages int32) *Source[T] {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Sprintf(`error creating AWS source: %v`, err))
	}
	return &Source[T]{
		client: sqs.NewFromConfig(cfg),
		params: &sqs.ReceiveMessageInput{
			QueueUrl:            queueUrl,
			MaxNumberOfMessages: maxMessages,
			WaitTimeSeconds:     20,
		},
	}
}

func (a *Source[T]) Receive(ctx context.Context) ([]T, error) {
	result, err := a.client.ReceiveMessage(ctx, a.params)
	if err != nil {
		return nil, fmt.Errorf(`error receiving AWS messages: %w`, err)
	}

	payloads := make([]T, 0, len(result.Messages))
	deletes := make([]types.DeleteMessageBatchRequestEntry, 0, len(result.Messages))
	for _, message := range result.Messages {
		deletes = append(deletes, types.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})

		path, err := decodeMessageBody[T](message.Body)
		if err != nil {
			return nil, fmt.Errorf(`error receiving AWS messages: error decoding message body: %w`, err)
		}
		payloads = append(payloads, path)
	}
	if len(deletes) > 0 {
		_, err = a.client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
			Entries:  deletes,
			QueueUrl: a.params.QueueUrl,
		})
		if err != nil {
			return nil, fmt.Errorf(`error receiving AWS messages: error deleting messages: %w`, err)
		}
	}
	return payloads, nil
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
