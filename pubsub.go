package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
	input    <-chan T
	subs     map[int]sub[T]
	subsLock sync.RWMutex
	capacity int
	nextID   int
	started  bool
	log      *slog.Logger
}

type sub[T any] struct {
	Channel    chan T
	IsBlocking bool
}

// New publisher. Specify capacity for each subscriber channels and optional logger
// You MUST call Start for the publisher to relay any items
func New[T any](capacity int, input <-chan T, optionalLogger ...*slog.Logger) *PubSub[T] {
	log := slog.Default().With("context", fmt.Sprintf("PubSub[%T]", *new(T)))
	if len(optionalLogger) > 0 {
		log = optionalLogger[0]
	}
	return &PubSub[T]{
		input:    input,
		subs:     make(map[int]sub[T]),
		capacity: capacity,
		log:      log}
}

// Start Publisher. Will run until context expires or the input channel is closed
func (ps *PubSub[T]) Start(ctx context.Context) {
	ps.subsLock.Lock()
	defer ps.subsLock.Unlock()

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
	ps.subsLock.RLock()
	defer ps.subsLock.RUnlock()

	if !ps.started {
		ps.log.Warn("Publisher not started, but has subscribers")
	}

	id := ps.nextID
	ps.nextID++

	rw := make(chan T, ps.capacity)
	ps.subs[id] = sub[T]{Channel: rw, IsBlocking: isBlocking}

	return rw, func() { go ps.unsubscribe(id) }
}

func (ps *PubSub[T]) unsubscribe(id int) {
	// get the subscription and start drain before final lock to remove
	ps.subsLock.RLock()
	sub, exists := ps.subs[id]
	ps.subsLock.RUnlock()

	if !exists {
		return
	}

	go func() {
		for range sub.Channel {
			// drain
		}
	}()

	ps.subsLock.Lock()
	defer ps.subsLock.Unlock()

	if sub, exists := ps.subs[id]; exists {
		delete(ps.subs, id)
		close(sub.Channel)
	}
}

func (ps *PubSub[T]) close() {
	ps.subsLock.Lock()
	defer ps.subsLock.Unlock()
	ps.log.Debug("stopping publisher")

	for id, sub := range ps.subs {
		delete(ps.subs, id)
		close(sub.Channel)
	}
}

func (ps *PubSub[T]) loop(ctx context.Context) {
	defer ps.close()
	ps.log.Debug("starting publisher")

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
	ps.subsLock.RLock()
	defer ps.subsLock.RUnlock()

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
			}
		}
	}
}
