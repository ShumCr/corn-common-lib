package queue

// TODO fix
import (
	"context"
	"encoding/json"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
)

type GooglePubSubClient interface {
	Topic(id string) *pubsub.Topic
	Subscription(id string) *pubsub.Subscription
	CreateTopic(ctx context.Context, topicID string) (*pubsub.Topic, error)
	CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
}

func NewGooglePubSubTopic(ctx context.Context, pubSubClient GooglePubSubClient, id string) (*pubsub.Topic, error) {
	topic := pubSubClient.Topic(id)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("topic does not exist")

	}

	return topic, nil
}

func NewGooglePubSubSubscription(ctx context.Context, pubSubClient GooglePubSubClient, subID string) (*pubsub.Subscription, error) {
	subscript := pubSubClient.Subscription(subID)
	exists, err := subscript.Exists(ctx)
	if err != nil {
		return subscript, err
	}
	if !exists {
		return nil, errors.New("subscription does not exist")
	}
	return subscript, nil
}

func ParseGooglePubSubMsg[T any](msg *pubsub.Message) (Message[T], error) {
	var t T
	err := json.Unmarshal(msg.Data, &t)
	if err != nil {
		return Message[T]{}, err
	}
	return Message[T]{Data: t, Msg: msg}, nil
}

type gcpPubSubPublisher[T any] struct {
	topic *pubsub.Topic
	log   logrus.Ext1FieldLogger
}

func NewGooglePubSubPublisher[T any](ctx context.Context, log logrus.Ext1FieldLogger, pubSubClient GooglePubSubClient, id string) (QueuePublisher[T], error) {

	topic, err := NewGooglePubSubTopic(ctx, pubSubClient, id)
	if err != nil {
		return nil, err
	}
	return &gcpPubSubPublisher[T]{topic: topic, log: log}, nil
}

func (pub *gcpPubSubPublisher[T]) Publish(ctx context.Context, data T) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = pub.topic.Publish(ctx, &pubsub.Message{Data: bytes}).Get(ctx)
	return err
}

type gcpPubSubSubscriber[T any] struct {
	sub     *pubsub.Subscription
	log     logrus.Ext1FieldLogger
	workers int
}




func NewGooglePubSubSubscriber[T any](ctx context.Context, log logrus.Ext1FieldLogger, pubSubClient GooglePubSubClient, subID string) (QueueSubscriber[T], error) {
	sub, err := NewGooglePubSubSubscription(ctx, pubSubClient, subID)
	if err != nil {
		return nil, err
	}
	return &gcpPubSubSubscriber[T]{sub: sub, log: log}, nil
}

func (sub *gcpPubSubSubscriber[T]) ReceiveCh(ctx context.Context) <-chan Message[T] {
	outChan := make(chan Message[T], sub.workers)
	go func() {
		err := sub.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			parsedMessage, err := ParseGooglePubSubMsg[T](msg)
			if err != nil {
				sub.log.WithError(err).Error("failed to unmarshal pubsub message into json")
				msg.Nack()
				return
			}
			outChan <- parsedMessage
		})
		if err != nil {
			sub.log.WithError(err).Panic("failed to subscribe to subscription")
		}
		close(outChan)
	}()
	return outChan

}
