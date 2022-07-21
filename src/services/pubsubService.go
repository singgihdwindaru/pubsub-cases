package services

type IPubsubService interface {
	PublishMsg() error
	PullMsg() error
}

type pubsubService struct{}

// PublishMsg implements IPubsubService
func (*pubsubService) PublishMsg() error {
	panic("unimplemented")
}

// PullMsg implements IPubsubService
func (*pubsubService) PullMsg() error {
	panic("unimplemented")
}

func NewPubSubService() IPubsubService {
	return &pubsubService{}
}
