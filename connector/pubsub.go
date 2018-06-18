package connector

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/Sirupsen/logrus"
	optional "github.com/bq/Go-Option/option"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type PubSubConnector struct {
	client    *pubsub.Client
	ctx       context.Context
	topicName string
	groupId   string
	topic     *pubsub.Topic
	out       chan *optional.Option
}

var oncePubsub sync.Once
var pubSubConnector PubSubConnector

type PubSubConnectorBehavior interface {
	PubMsg(metadata map[string]string, data ...interface{}) error
	SubMsg(*pubsub.Subscription) <-chan *optional.Option
	CreateSubscription() (*pubsub.Subscription, error)
}

func NewPubSubConnector(credentialsPath, topicName, projectID, groupId string) *PubSubConnector {
	oncePubsub.Do(func() {
		pubSubConnector.ctx = context.Background()

		if emu := os.Getenv("PUBSUB_EMULATOR_HOST"); emu != "" {
			client, err := pubsub.NewClient(pubSubConnector.ctx, projectID)

			if err != nil {
				log.Fatal(err)
			}

			pubSubConnector.client = client
			pubSubConnector.topicName = topicName

			exist, e := client.Topic(topicName).Exists(pubSubConnector.ctx)
			if e != nil {
				log.Fatal(e.Error())
			}

			if !exist {
				_, err := client.CreateTopic(pubSubConnector.ctx, topicName)
				if err != nil {
					log.Fatal("Topic " + topicName + " can not be created. " + err.Error())
				}
			}

			pubSubConnector.topic = client.Topic(topicName)
			pubSubConnector.out = make(chan *optional.Option)
			if groupId == "" {
				pubSubConnector.groupId, _ = os.Hostname()
			} else {
				pubSubConnector.groupId = groupId
			}

		} else {
			if credentialsPath != "" {
				jsonKey, err := ioutil.ReadFile(path.Join(credentialsPath, "keyfile.json"))

				if err != nil {
					log.Fatal(err)
				}

				conf, err := google.JWTConfigFromJSON(
					jsonKey,
					pubsub.ScopePubSub,
				)

				if err != nil {
					log.Fatal(err)
				}

				client, err := pubsub.NewClient(
					pubSubConnector.ctx,
					projectID,
					option.WithTokenSource(conf.TokenSource(pubSubConnector.ctx)),
				)

				pubSubConnector.client = client
				pubSubConnector.topicName = topicName

				exist, e := client.Topic(topicName).Exists(pubSubConnector.ctx)
				if e != nil {
					log.Fatal(e.Error())
				}

				if !exist {
					_, err := client.CreateTopic(pubSubConnector.ctx, topicName)
					if err != nil {
						log.Fatal("Topic " + topicName + " can not be created. " + err.Error())
					}
				}

				pubSubConnector.topic = client.Topic(topicName)
				pubSubConnector.out = make(chan *optional.Option)
				if groupId == "" {
					pubSubConnector.groupId, _ = os.Hostname()
				} else {
					pubSubConnector.groupId = groupId
				}

			} else {
				log.Fatal("Missing cloudstorage credentials path")
			}
		}
	})

	return &pubSubConnector
}

func (pubSubConnector *PubSubConnector) PubMsg(metadata map[string]string, data ...interface{}) (err error) {

	if jsonMsg, errMarshal := json.Marshal(data); err == nil {

		msg := &pubsub.Message{
			Data:       jsonMsg,
			Attributes: metadata,
		}

		pubSubConnector.topic.Publish(pubSubConnector.ctx, msg)

	} else {
		logrus.Error(errMarshal.Error())
		err = errMarshal
	}

	return err
}

func (pubSubConnector *PubSubConnector) CreateSubscription() (*pubsub.Subscription, error) {
	subscription, err := pubSubConnector.client.CreateSubscription(pubSubConnector.ctx, pubSubConnector.groupId, pubsub.SubscriptionConfig{Topic: pubSubConnector.topic})
	if err != nil {
		logrus.Error(err.Error())
		subName, _ := os.Hostname()
		subscription = pubSubConnector.client.Subscription(subName)
	}

	return subscription, err
}

func (pubSubConnector *PubSubConnector) SubMsg(subscription *pubsub.Subscription) <-chan *optional.Option {

	go func() {
		subscription.Receive(pubSubConnector.ctx, func(ctx context.Context, m *pubsub.Message) {
			m.Ack() // Acknowledge that we've consumed the message.
			pubSubConnector.out <- optional.Of(m)
		})
	}()
	return pubSubConnector.out
}
