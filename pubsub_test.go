package pubsub

import (
	"context"
	"testing"
)

func TestPubsubAfterCancelDoesNotBlockOthers(t *testing.T) {
	input := make(chan string)
	ps := New(0, input)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps.Start(ctx)

	_, cancel1 := ps.Subscribe()
	sub2, cancel2 := ps.Subscribe()
	defer cancel2()

	input <- "a"

	// 2nd message must not block
	go func() {
		input <- "b"
	}()

	cancel1()

	// sub2 should be able to read both messages
	<-sub2
	<-sub2
}
