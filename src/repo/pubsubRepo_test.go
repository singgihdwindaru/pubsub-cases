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
	gac := "../../keyPubSub.json"
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
	suite.Run(t, new(pubsubRepoSuite))
}
func (s *pubsubRepoSuite) TestConcurrent() {
	// res1 := []string{`{"code":0,"message":"Berhasil","data":"nil"}`, `{"code":1,"message":"Berhasil","data":"nil"}`, `{"code":2,"message":"Berhasil","data":"nil"}`, `{"code":3,"message":"Berhasil","data":"nil"}`, `{"code":4,"message":"Berhasil","data":"nil"}`}
	// res1 := map[int]int{
	// 	0: 5,
	// 	1: 5,
	// 	2: 5,
	// 	3: 5,
	// 	4: 5,
	// }
	testTable := []struct {
		name       string
		wantResult interface{}
		sut        func() error
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
					go rightMessage(wg, param, i, s) // publishing right format messages
					wg.Add(1)
					go wrongMessage(wg, i, s) // publishing wrong format messages
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
							ackDeadLetterSub(s, message)
						},
					},
					{
						SubscriberId: s.subs,
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							ackMainSub(message, s)
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
		{
			name: "PublishingWhileSubscribing",
			sut: func() (err error) {
				wg := &sync.WaitGroup{}
				subs := []models.SubscriberConfig{
					{
						SubscriberId: s.subsDL,
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							ackDeadLetterSub(s, message)
						},
					},
					{
						SubscriberId: s.subs,
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							ackMainSub(message, s)
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

				param := models.PubSubModels{
					Message: "Berhasil",
					Data:    "nil",
				}
				for i := 0; i < 5; i++ {
					wg.Add(1)
					go rightMessage(wg, param, i, s) // publishing right format messages
					wg.Add(1)
					go wrongMessage(wg, i, s) // publishing wrong format messages
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

func ackMainSub(message *pubsub.Message, s *pubsubRepoSuite) {
	param := models.PubSubModels{}
	err := json.Unmarshal(message.Data, &param)
	if err != nil {
		fmt.Printf("%s : Fail Consuming %s\n", s.subs, string(message.Data))
		message.Nack()
		return
	}
	fmt.Printf("%s : Success Consuming %s\n", s.subs, string(message.Data))
	message.Ack()
}

func ackDeadLetterSub(s *pubsubRepoSuite, message *pubsub.Message) {
	fmt.Printf("%s : Ack-ing %s\n", s.subsDL, string(message.Data))
	message.Ack()
}

func wrongMessage(swg *sync.WaitGroup, iteration int, s *pubsubRepoSuite) (err error) {
	defer swg.Done()

	data := fmt.Sprintf("Hello world %d", iteration)
	err = s.pubsubRepo.PublishMessage(context.Background(), data)
	if err == nil {
		fmt.Printf("Published a message : %v\n", data)
	}
	return
}

func rightMessage(swg *sync.WaitGroup, param models.PubSubModels, iteration int, s *pubsubRepoSuite) (err error) {
	defer swg.Done()
	param.Code = iteration
	data, _ := json.Marshal(param)

	err = s.pubsubRepo.PublishMessage(context.Background(), string(data))
	if err == nil {
		fmt.Printf("Published a message : %v\n", string(data))
	}
	return
}
