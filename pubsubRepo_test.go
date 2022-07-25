package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"pubsub-cases/src/models"
	"pubsub-cases/src/repo"
	"pubsub-cases/src/services"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

// type pubsubRepoSuite struct {
// 	suite.Suite
// 	pubsubRepo repo.IPubSubRepo
// }

// func (s *pubsubRepoSuite) SetupSuite() {}
func TestIntgPubSub(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIAL", "keyPubSub.json")
	t.Setenv("PROJECT_ID", "propane-galaxy-168212")
	t.Setenv("TOPIC", "hello-pubsub")
	t.Setenv("SUBSCRIBER", "hello-pubsub-sub")
	t.Setenv("SUBSCRIBER_DEAD_LETTER", "propane-galaxy-168212")

	_pubsubClient, err := pubsub.NewClient(context.Background(), os.Getenv("PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIAL")))
	assert.NoError(t, err)
	defer _pubsubClient.Close()

	repo := repo.NewPubSubRepo(_pubsubClient, os.Getenv("TOPIC"))
	serv := services.NewPubSubService(repo)

	testTable := []struct {
		name string
		sut  func() error
	}{
		{
			name: "Publish",
			sut: func() error {
				param := models.PubSubModels{
					Code:    0,
					Message: "Berhasil",
					Data:    "nil",
				}
				for i := 0; i < 5; i++ {
					param.Code = i
					data, _ := json.Marshal(param)
					jsonString := string(data)
					err = serv.PublishMsg(context.Background(), jsonString)
				}
				for i := 0; i < 5; i++ {
					err = serv.PublishMsg(context.Background(), fmt.Sprintf("Hello world %d", i))
				}
				return err
			},
		},
		{
			name: "Subscribe",
			sut: func() error {
				cfg := models.SubscriberConfig{
					SubscriberId: os.Getenv("SUBSCRIBER"),
					Concurrency:  1,
					Handler: func(ctx context.Context, message *pubsub.Message) {
						param := models.PubSubModels{}
						err := json.Unmarshal(message.Data, &param)
						if err != nil {
							fmt.Println(string(message.Data))
							message.Nack()
							return
						}
						fmt.Println(string(message.Data))
						message.Ack()
					},
				}
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				err = serv.PullMsg(ctx, cfg)
				return err
			},
		},
	}

	for _, tests := range testTable {
		t.Run(tests.name, func(t *testing.T) {
			assert.NoError(t, tests.sut())
		})
	}
}

func TestIntgPubSubConcurent(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIAL", "keyPubSub.json")
	t.Setenv("PROJECT_ID", "propane-galaxy-168212")
	t.Setenv("TOPIC", "Hello-Concurrent")
	t.Setenv("SUBSCRIBER", "Hello-Concurrent-sub")
	t.Setenv("DEAD_SUB_ID", "Hello-Concurrent-dead-sub")

	_pubsubClient, err := pubsub.NewClient(context.Background(), os.Getenv("PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIAL")))
	assert.NoError(t, err)
	defer _pubsubClient.Close()

	repo := repo.NewPubSubRepo(_pubsubClient, os.Getenv("TOPIC"))
	serv := services.NewPubSubService(repo)

	testTable := []struct {
		name string
		sut  func() error
	}{
		{
			name: "ConcPublish",
			sut: func() error {
				return nil
			},
		},
		{
			name: "ConcSubscribe",
			sut: func() error {
				wg := &sync.WaitGroup{}
				subs := []models.SubscriberConfig{
					{
						SubscriberId: os.Getenv("DEAD_SUB_ID"),
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							fmt.Printf("%s : Ack-ing %s\n", os.Getenv("DEAD_SUB_ID"), string(message.Data))
							message.Ack()
						},
					},
					{
						SubscriberId: os.Getenv("SUBSCRIBER"),
						Concurrency:  1,
						Handler: func(ctx context.Context, message *pubsub.Message) {
							param := models.PubSubModels{}
							err := json.Unmarshal(message.Data, &param)
							if err != nil {
								fmt.Printf("%s : Fail Consuming %s\n", os.Getenv("SUBSCRIBER"), string(message.Data))
								message.Nack()
								return
							}
							fmt.Printf("%s : Success Consuming %s\n", os.Getenv("SUBSCRIBER"), string(message.Data))
							message.Ack()
						},
					},
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				for _, s := range subs {
					wg.Add(1)
					go func(swg *sync.WaitGroup, cfg models.SubscriberConfig) error {
						defer swg.Done()
						err = serv.PullMsg(ctx, cfg)
						return err
					}(wg, s)
				}
				wg.Wait()
				return nil
			},
		},
	}

	for _, tests := range testTable {
		t.Run(tests.name, func(t *testing.T) {
			assert.NoError(t, tests.sut())
		})
	}
}

func TestIntgPublishMsg(t *testing.T) {
	assert := assert.New(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIAL", "keyPubSub.json")
	t.Setenv("PROJECT_ID", "propane-galaxy-168212")
	// t.Setenv("TOPIC", "hello-pubsub")
	t.Setenv("TOPIC", "Hello-Concurrent")
	_pubsubClient, err := pubsub.NewClient(context.Background(), os.Getenv("PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIAL")))
	assert.NoError(err)
	defer _pubsubClient.Close()

	repo := repo.NewPubSubRepo(_pubsubClient, os.Getenv("TOPIC"))
	serv := services.NewPubSubService(repo)
	param := models.PubSubModels{
		Code:    0,
		Message: "Berhasil",
		Data:    "nil",
	}
	// serv.PublishMsg(context.Background(), string(jsonString))
	for i := 0; i < 5; i++ {
		param.Code = i
		data, _ := json.Marshal(param)
		jsonString := string(data)
		serv.PublishMsg(context.Background(), jsonString)
	}
	for i := 0; i < 5; i++ {
		serv.PublishMsg(context.Background(), fmt.Sprintf("Hello world %d", i))
	}
}
func TestIntgPullMsg(t *testing.T) {
	assert := assert.New(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIAL", "keyPubSub.json")
	t.Setenv("PROJECT_ID", "propane-galaxy-168212")
	t.Setenv("SUBSCRIBER", "hello-pubsub-sub")
	t.Setenv("SUBSCRIBER_DEAD_LETTER", "propane-galaxy-168212")
	t.Setenv("TOPIC", "hello-pubsub")

	_pubsubClient, err := pubsub.NewClient(context.Background(), os.Getenv("PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIAL")))
	assert.NoError(err)
	defer _pubsubClient.Close()

	repo := repo.NewPubSubRepo(_pubsubClient, os.Getenv("TOPIC"))
	serv := services.NewPubSubService(repo)
	cfg := models.SubscriberConfig{
		SubscriberId: os.Getenv("SUBSCRIBER"),
		Concurrency:  1,
		Handler: func(ctx context.Context, message *pubsub.Message) {
			param := models.PubSubModels{}
			err := json.Unmarshal(message.Data, &param)
			if err != nil {
				fmt.Println(string(message.Data))
				message.Nack()
				return
			}
			fmt.Println(string(message.Data))
			message.Ack()
		},
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = serv.PullMsg(ctx, cfg)
	assert.NoError(err)
}
