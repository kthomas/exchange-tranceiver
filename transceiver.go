package main

import (
	"os"
	"sync"
	"time"

	"github.com/kthomas/go-logger"
	"github.com/streadway/amqp"
	"errors"
)

type StreamingDataSource struct {
	AmqpBindingKey string
	AmqpExchange string
	AmqpExchangeType string
	AmqpExchangeDurable bool
	AmqpQueue string

	Stream func(chan *[]byte) error  // implementations should be blocking
}

type Transceiver struct {
	DataSource *StreamingDataSource

	log *logger.Logger

	queue chan *[]byte

	rmqConn *amqp.Connection
	rmqChan *amqp.Channel

	wg sync.WaitGroup
}

func (t *Transceiver) bindQueue() error {
	err := t.declareQueue()
	if err == nil {
		t.log.Debugf("Binding %s queue to routing key %s in %s AMQP exchange", t.DataSource.AmqpQueue, t.DataSource.AmqpBindingKey, t.DataSource.AmqpExchange)
		err = t.rmqChan.QueueBind(t.DataSource.AmqpQueue, t.DataSource.AmqpBindingKey, t.DataSource.AmqpExchange, false, nil)
		t.log.Debugf("Bound %s queue to %s routing key in %s AMQP exchange", t.DataSource.AmqpQueue, t.DataSource.AmqpBindingKey, t.DataSource.AmqpExchange)
	}
	return err
}

func (t *Transceiver) declareQueue() error {
	t.log.Debugf("Declaring %s queue in %s AMQP exchange", t.DataSource.AmqpQueue, t.DataSource.AmqpBindingKey)
	_, err := t.rmqChan.QueueDeclare(
		t.DataSource.AmqpQueue,	 // name
		true,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,  // arguments
	)
	if err != nil {
		t.log.Warningf("Failed to declare %s queue in AMQP exchange", t.DataSource.AmqpQueue, t.DataSource.AmqpExchange)
	}
	return err
}

func (t *Transceiver) loop() error {
	for {
		select {
		case msg := <-t.queue:
			t.publish(msg)

		case <-Shutdown.Done():
			t.log.Debugf("Closing transceiver on shutdown")
			return nil
		}
	}

	return nil
}

func (t *Transceiver) publish(msg *[]byte) error {
	t.log.Debugf("Publishing %v-byte message; %s", len(*msg), msg)

	payload := amqp.Publishing{
		Body: *msg,
		ContentType: "application/json",
	}

	err := t.rmqChan.Publish(
		t.DataSource.AmqpExchange,  // exchange
		t.DataSource.AmqpBindingKey,  // binding key
		true,  // mandatory
		false,  // immediate
		payload,  // payload
	)

	if err != nil {
		t.log.Errorf("Error attempting to publish %v-byte message: %s", len(*msg), msg)
	} else {
		t.log.Debugf("Published %v-byte message to binding key %s: %s", len(*msg), t.DataSource.AmqpBindingKey, msg)
	}

	return err
}

func (t *Transceiver) stream() {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.DataSource.Stream(t.queue)
	}()
}

func NewTransceiver(lg *logger.Logger, dataSource *StreamingDataSource) *Transceiver {
	t := new(Transceiver)
	t.log = lg.Clone()

	t.DataSource = dataSource

	t.queue = make(chan *[]byte, 512)

	url := os.Getenv("AMQP_URL")
	if url == "" {
		err := errors.New("Transceiver environment not configured with AMQP_URL")
		t.log.PanicOnError(err, "")
	}
	heartbeat := time.Duration(60) * time.Millisecond

	rmqConn, rmqChan, err := setupAmqp(url, heartbeat, t.DataSource.AmqpExchange, t.DataSource.AmqpExchangeType, t.DataSource.AmqpExchangeDurable)
	if err == nil {
		t.rmqConn = rmqConn
		t.rmqChan = rmqChan
		t.log.Debugf("AMQP connection %s opened with channel %s on exchange %s", t.rmqConn, t.rmqChan, t.DataSource.AmqpExchange)

		t.bindQueue()
	}

	return t
}

func (t *Transceiver) Run() error {
	if t.rmqChan != nil {
		defer t.rmqChan.Close()
	}
	if t.rmqConn != nil {
		defer t.rmqConn.Close()
	}

	t.stream()
	err := t.loop()

	if err == nil {
		t.log.Info("Transceiver exited cleanly")
	} else {
		t.log.Warningf("Transceiver exited; %s", err)
	}

	return err
}
