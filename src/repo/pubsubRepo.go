package repo

import "context"

type IPubSubRepo interface {
	PublishMessage(ctx context.Context, message interface{}) error
}
type pubsubRepo struct{}

// PublishMessage implements IPubSubRepo
func (*pubsubRepo) PublishMessage(ctx context.Context, message interface{}) error {
	panic("unimplemented")
}

func NewPubSubRepo() IPubSubRepo {
	return &pubsubRepo{}
}
