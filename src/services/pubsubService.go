package services

import (
	"context"
	"pubsub-cases/src/models"
	"pubsub-cases/src/repo"
)

type IPubsubService interface {
	PublishMsg(ctx context.Context, message string) error
	PullMsg(ctx context.Context, config models.SubscriberConfig) error
}

type pubsubService struct {
	pubsubRepo repo.IPubSubRepo
}

func NewPubSubService(pubsubRepo repo.IPubSubRepo) IPubsubService {
	return &pubsubService{
		pubsubRepo: pubsubRepo,
	}
}

// PublishMsg implements IPubsubService
func (p *pubsubService) PublishMsg(ctx context.Context, message string) (err error) {
	err = p.pubsubRepo.PublishMessage(ctx, message)
	return
}

// PullMsg implements IPubsubService
func (p *pubsubService) PullMsg(ctx context.Context, config models.SubscriberConfig) (err error) {
	err = p.pubsubRepo.PullMessage(ctx, config)
	return
}
