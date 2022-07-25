package repo

import (
	"context"
	"fmt"
	"log"
	"pubsub-cases/src/models"

	"cloud.google.com/go/pubsub"
)

type IPubSubRepo interface {
	PublishMessage(ctx context.Context, message interface{}) error
	PullMessage(ctx context.Context, config models.SubscriberConfig) error
}
type pubsubRepo struct {
	PubsubClient *pubsub.Client
	PubSubTopic  string
}

func NewPubSubRepo(pubsubClient *pubsub.Client, topicName string) IPubSubRepo {
	pub := &pubsubRepo{
		PubsubClient: pubsubClient,
		PubSubTopic:  topicName,
	}
	return pub
}

// PullMessage implements IPubSubRepo
func (p *pubsubRepo) PullMessage(ctx context.Context, config models.SubscriberConfig) (err error) {
	sub := p.PubsubClient.Subscription(config.SubscriberId)
	sub.ReceiveSettings.MaxOutstandingMessages = config.Concurrency
	err = sub.Receive(ctx, config.Handler)
	if err != nil {
		log.Fatal(err)
	}
	return

}

// PublishMessage implements IPubSubRepo
func (p *pubsubRepo) PublishMessage(ctx context.Context, message interface{}) error {
	t := p.PubsubClient.Topic(p.PubSubTopic)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(message.(string)),
	})

	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
	return nil
}
