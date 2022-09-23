package pubsub

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type Publisher[T any] interface {
	// subscribe to future published items
	// 2nd return element is for cancelling the subscription
	Subscribe() (<-chan T, func())

	// subscribe to future published items non-blocking
	// i.e. if the consumer is not ready the element will be discarded
	// 2nd return element is for cancelling the subscription
	SubscribeNonBlocking() (<-chan T, func())
}

type PubSub[T any] struct {
	sync.Mutex
	input    <-chan T
	subs     map[int]sub[T]
	capacity int
	nextID   int
	started  bool
}

type sub[T any] struct {
	Channel    chan T
	IsBlocking bool
}

// New publisher. Specify capacity for subscriber channels
// You MUST call Start for the publisher to relay any items
func New[T any](capacity int, input <-chan T) *PubSub[T] {
	return &PubSub[T]{
		input:    input,
		subs:     make(map[int]sub[T]),
		capacity: capacity}
}

// Start Publisher. Will run until context expires or the input channel is closed
func (ps *PubSub[T]) Start(ctx context.Context) {
	ps.Lock()
	defer ps.Unlock()

	if ps.started {
		return
	}

	ps.started = true
	go ps.loop(ctx)
}

func (ps *PubSub[T]) Subscribe() (ch <-chan T, cancel func()) {
	return ps.subscribe(true)
}

func (ps *PubSub[T]) SubscribeNonBlocking() (ch <-chan T, cancel func()) {
	return ps.subscribe(false)
}

func (ps *PubSub[T]) subscribe(isBlocking bool) (<-chan T, func()) {
	ps.Lock()
	defer ps.Unlock()

	if !ps.started {
		logrus.Warnf("Publisher[%T] not started, but has subscribers", *new(T))
	}

	id := ps.nextID
	ps.nextID++

	rw := make(chan T, ps.capacity)
	ps.subs[id] = sub[T]{Channel: rw, IsBlocking: isBlocking}

	return rw, func() { ps.unsubscribe(id) }
}

func (ps *PubSub[T]) unsubscribe(id int) {
	ps.Lock()
	defer ps.Unlock()

	if sub, exists := ps.subs[id]; exists {
		delete(ps.subs, id)
		close(sub.Channel)
	}
}

func (ps *PubSub[T]) close() {
	ps.Lock()
	defer ps.Unlock()

	for id, sub := range ps.subs {
		delete(ps.subs, id)
		close(sub.Channel)
	}
}

func (ps *PubSub[T]) loop(ctx context.Context) {
	defer ps.close()

	for {
		select {
		case <-ctx.Done():
			return
		case x, ok := <-ps.input:
			if !ok {
				return
			}
			ps.publish(ctx, x)
		}
	}
}

func (ps *PubSub[T]) publish(ctx context.Context, x T) {
	ps.Lock()
	defer ps.Unlock()

	for _, sub := range ps.subs {
		if sub.IsBlocking {
			select {
			case <-ctx.Done():
				return
			case sub.Channel <- x:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case sub.Channel <- x:
			default:
				return
			}
		}
	}
}
