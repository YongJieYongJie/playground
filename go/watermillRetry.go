package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var (
	logger = watermill.NewStdLogger(false, false)
)

const (
	topicIn  = "TOPIC_IN"
	topicOut = "TOPIC_OUT"
	topicDLQ = "TOPIC_DLQ"
)

func watermillRetryDemo() {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{OutputChannelBuffer: 1},
		logger,
	)

	// Middlewares for DLQ and retry. Note: DLQ should come before retry, meaning
	// that messages that fail retry will go into DLQ.
	dlq, _ := middleware.PoisonQueue(pubSub, topicDLQ)
	retry := middleware.Retry{
		MaxRetries:      3,
		InitialInterval: time.Second * 1,
		MaxInterval:     time.Minute * 10,
		Multiplier:      2,
		MaxElapsedTime:  time.Hour, // Do not retry if elapsed time > an hour.
		Logger:          logger,
	}.Middleware

	// Creating and configuring router with middlewares.
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}
	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(dlq, retry)

	// Start simulation of 10 mesasges.
	go simulateTenIncomingMessages(pubSub)

	// Add fake handler that fails every message with UUID that is a multiple of
	// 3.
	router.AddHandler(
		"struct_handler",
		topicIn,
		pubSub,
		topicOut,
		pubSub,
		failMultipleOfThree,
	)

	// Add sink for outgoing message.
	router.AddNoPublisherHandler(
		"print_outgoing_messages",
		topicOut,
		pubSub,
		msgPrinter{prefix: "Outgoing"}.new(),
	)

	// Add sink for DLQ.
	router.AddNoPublisherHandler(
		"print_dlq_messages",
		topicDLQ,
		pubSub,
		msgPrinter{prefix: "DLQ"}.new(),
	)

	// Start the router.
	ctx := context.Background()
	if err := router.Run(ctx); err != nil {
		panic(err)
	}
}

func simulateTenIncomingMessages(publisher message.Publisher) {
	numMsg := 10
	delayBtwMsg := time.Second * 1

	for i := 1; i <= numMsg; i++ {
		time.Sleep(delayBtwMsg)

		uuid := strconv.FormatInt(int64(i), 10)
		msgContent := []byte(fmt.Sprint("Hello, I'm message #", i))
		msg := message.NewMessage(uuid, msgContent)

		if err := publisher.Publish(topicIn, msg); err != nil {
			panic(err)
		}
		log.Println("Simulating incoming message:", i)
	}
}

type msgPrinter struct {
	prefix string
}

func (p msgPrinter) new() message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		log.Printf(
			"[%s] UUID: %s | Payload: %s | Metadata: %v\n",
			p.prefix, msg.UUID, string(msg.Payload), msg.Metadata,
		)
		return nil
	}
}

func failMultipleOfThree(msg *message.Message) ([]*message.Message, error) {
	log.Println("[Handler] Received message", msg.UUID,
		"| processing for 5 seconds, simulating failure if UUID is multiple of 3...")

	time.Sleep(time.Second * 5)

	msgIdx, err := strconv.ParseInt(msg.UUID, 10, 0)
	if err != nil {
		panic(err)
	}
	if msgIdx%3 == 0 {
		log.Println("[Handler] Returning error for message", msg.UUID)
		return nil, fmt.Errorf(
			"simulated error for message with UUID that is multiple of 3")
	}
	log.Println("[Handler] Done processing message", msg.UUID,
		"without errors")
	return message.Messages{msg}, nil
}
