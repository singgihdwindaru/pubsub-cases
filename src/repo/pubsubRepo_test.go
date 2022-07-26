package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"pubsub-cases/src/models"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/option"
)

type pubsubRepoSuite struct {
	suite.Suite
	pubsubRepo   IPubSubRepo
	subs, subsDL string
	pubsubClient *pubsub.Client
	timeout      time.Duration
}

func (s *pubsubRepoSuite) SetupSuite() {
	gac := "keyPubSub.json"
	pid := "propane-galaxy-168212"
	tid := "hello-pubsub"
	s.subs = "hello-pubsub-sub"
	s.subsDL = "hello-pubsub-dead-letter"
	pClient, err := pubsub.NewClient(context.Background(), pid, option.WithCredentialsFile(gac))
	if err != nil {
		log.Fatal(err)
	}
	s.pubsubClient = pClient
	s.pubsubRepo = NewPubSubRepo(pClient, tid)
	s.timeout = 10 * time.Second
}
func (s *pubsubRepoSuite) TearDownSuite() {
	defer s.pubsubClient.Close()

}
func TestIntegrationPubSub(t *testing.T) {
	// create an instance of our test object
	suite.Run(t, new(pubsubRepoSuite))
}
func (s *pubsubRepoSuite) TestConcurrent() {
	testTable := []struct {
		name string
		sut  func() error
	}{
		{
			name: "Publish",
			sut: func() (err error) {
				wg := &sync.WaitGroup{}
				param := models.PubSubModels{
					Message: "Berhasil",
					Data:    "nil",
				}
				for i := 0; i < 5; i++ {
					wg.Add(1)
					go func(swg *sync.WaitGroup, iteration int) (err error) {
						defer swg.Done()
						param.Code = iteration
						data, _ := json.Marshal(param)
						jsonString := string(data)
						// publishing 5 right format messages
						err = s.pubsubRepo.PublishMessage(context.Background(), jsonString)
						return
					}(wg, i)

					wg.Add(1)
					go func(swg *sync.WaitGroup, iteration int) (err error) {
						defer swg.Done()
						// publishing 5 wrong format messages
						err = s.pubsubRepo.PublishMessage(context.Background(), fmt.Sprintf("Hello world %d", iteration))
						return
					}(wg, i)
				}
				wg.Wait()
				return
			},
		},
		{
			name: "Subscribe",
			sut: func() (err error) {
				wg := &sync.WaitGroup{}
				subs := []models.SubscriberConfig{
					{
						SubscriberId: s.subsDL,
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							fmt.Printf("%s : Ack-ing %s\n", s.subsDL, string(message.Data))
							message.Ack()
						},
					},
					{
						SubscriberId: s.subs,
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							param := models.PubSubModels{}
							err := json.Unmarshal(message.Data, &param)
							if err != nil {
								fmt.Printf("%s : Fail Consuming %s\n", s.subs, string(message.Data))
								message.Nack()
								return
							}
							fmt.Printf("%s : Success Consuming %s\n", s.subs, string(message.Data))
							message.Ack()
						},
					},
				}
				ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
				defer cancel()
				for _, sub := range subs {
					wg.Add(1)
					go func(swg *sync.WaitGroup, cfg models.SubscriberConfig) error {
						defer swg.Done()
						err = s.pubsubRepo.PullMessage(ctx, cfg)
						return err
					}(wg, sub)
				}
				wg.Wait()
				return
			},
		},
	}
	for _, tests := range testTable {
		s.Run(tests.name, func() {
			s.NoError(tests.sut())
		})
	}
}
