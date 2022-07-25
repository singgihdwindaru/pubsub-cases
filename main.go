package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"pubsub-cases/src/repo"
	"pubsub-cases/src/services"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

var _pubsubClient *pubsub.Client

func initPubsub() (err error) {
	_pubsubClient, err = pubsub.NewClient(context.Background(), os.Getenv("PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIAL")))
	if err != nil {
		log.Fatal(err)
	}
	defer _pubsubClient.Close()
	return
}
func main() {
	initPubsub()
	repo := repo.NewPubSubRepo(_pubsubClient, os.Getenv("TOPIC"))
	serv := services.NewPubSubService(repo)

	for i := 0; i < 5; i++ {
		serv.PublishMsg(context.Background(), fmt.Sprintf("Hello world %d", i))
	}
}
