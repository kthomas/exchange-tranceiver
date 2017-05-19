package main

import (
	"sync"

	"github.com/kthomas/go-amqputil"
	"github.com/kthomas/go-logger"
	"github.com/streadway/amqp"
)

type StreamingDataSource struct {
	DestinationAmqpConfig *amqputil.AmqpConfig
	Stream func(chan *[]byte) error  // implementations should be blocking
}

type Transceiver struct {
	dataSource *StreamingDataSource
	publisher *amqputil.Publisher

	log *logger.Logger

	queue chan *[]byte

	rmqConn *amqp.Connection
	rmqChan *amqp.Channel

	wg sync.WaitGroup
}

func (t *Transceiver) loop() error {
	for {
		select {
		case msg := <-t.queue:
			t.publisher.Publish(msg)

		case <-Shutdown.Done():
			t.log.Debugf("Closing transceiver on shutdown")
			return nil
		}
	}

	return nil
}

func (t *Transceiver) stream() {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.dataSource.Stream(t.queue)
	}()
}

func NewTransceiver(lg *logger.Logger, dataSource *StreamingDataSource) *Transceiver {
	t := new(Transceiver)
	t.log = lg.Clone()
	t.dataSource = dataSource
	t.publisher, _ = amqputil.NewPublisher(lg, t.dataSource.DestinationAmqpConfig)
	t.queue = make(chan *[]byte, 512)
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
