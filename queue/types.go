package queue


//Types
import "context"

type QueuePublisher[T any] interface {
	Publish(ctx context.Context, data T) error
}

type UnderlyingMessage interface {
	Ack()
	Nack()
}

type Message[T any] struct {
	Data T
	Msg  UnderlyingMessage
}

func (m *Message[T]) Ack() {
	m.Msg.Ack()
}

func (m *Message[T]) Nack() {
	m.Msg.Nack()
}

type QueueSubscriber[T any] interface {
	ReceiveCh(ctx context.Context) <-chan Message[T]
}
