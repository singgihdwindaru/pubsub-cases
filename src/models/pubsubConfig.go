package models

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type SubscriberConfig struct {
	SubscriberId string
	Concurrency  int
	Handler      func(ctx context.Context, message *pubsub.Message)
}
