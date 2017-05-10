package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kthomas/go-logger"
	"github.com/streadway/amqp"
)

var (
	Shutdown context.Context
	CancelF context.CancelFunc
	Closing uint32
	WaitGroup sync.WaitGroup
	Log *logger.Logger

	CurrencyPairs = map[string]*StreamingDataSource{
		"BTC-USD": GdaxFactory("BTC", "USD"),
		"ETH-USD": GdaxFactory("ETH", "USD"),
		"ETH-BTC": GdaxFactory("ETH", "BTC"),

		"USD-CNY": OandaFactory("USD", "CNY"),
	}
)

func bootstrap() {
	Shutdown, CancelF = context.WithCancel(context.Background())

	setupLogging()
	handleSignals()
}

func setupLogging() {
	prefix := "exchange-transceiver"
	lvl := "WARNING"
	console := true

	Log = logger.NewLogger(prefix, lvl, console)
	Log.Infof("Logging initialized; log level: %s", lvl)
}

func handleSignals() {
	Log.Debug("Installing SIGINT and SIGTERM signal handlers")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	WaitGroup.Add(1)
	go func() {
		defer WaitGroup.Done()
		select {
		case sig := <-sigs:
			Log.Infof("Received signal: %s", sig)
			shutdown()
		case <-Shutdown.Done():
			close(sigs)
		}
	}()
}

func main() {
	bootstrap()
	run()

	WaitGroup.Wait()
	Log.Infof("Exiting exchange transceiver")
}

func run() {
	for _, pair := range CurrencyPairs {
		runTransceiver(pair)
	}
}

func runTransceiver(dataSource *StreamingDataSource) {
	Log.Infof("Starting exchange transceiver with streaming data source %s", dataSource)

	WaitGroup.Add(1)
	go func() {
		defer WaitGroup.Done()

		maxAttempts := 10  // transceiver's max # of retry attempts after failure before exiting
		attempt := 0
		var err error
		for !shuttingDown() && (attempt < maxAttempts) {
			attempt += 1
			t := NewTransceiver(Log, dataSource)
			err = t.Run()
		}

		if !shuttingDown() {
			Log.Errorf("Forcing shutdown of exchange transceiver due to error; %s", err)
			shutdown()
		}
	}()
}

func setupAmqp(url string, heartbeat time.Duration, exchange string, exchangeType string, durable bool) (*amqp.Connection, *amqp.Channel, error) {
	Log.Debugf("Dialing AMQP: %s; heartbeat interval: %s", url, heartbeat)

	cfg := amqp.Config{
		Heartbeat: heartbeat,
	}

	var err error
	conn, err := amqp.DialConfig(url, cfg)
	if err != nil {
		return nil, nil, err
	}

	Log.Debugf("AMQP connected; localaddr: %s", conn.LocalAddr())

	ch, err := conn.Channel()
	if err != nil {
		return conn, nil, err
	}

	Log.Debugf("Declaring AMQP %s %s exchange: %s", durable, exchangeType, exchange)
	err = ch.ExchangeDeclare(
		exchange,	// name
		exchangeType,	// type
		durable,	// durable
		false,		// auto-deleted
		false,		// internal
		false,		// no-wait
		nil,		// arguments
	)
	if err != nil {
		Log.Warningf("Failed to declare AMQP %s %s exchange: %s; %s", durable, exchangeType, exchange, err)
		return conn, ch, err
	}

	return conn, ch, nil
}

func shutdown() {
	if atomic.AddUint32(&Closing, 1) == 1 {
		Log.Debug("Shutdown broadcast")
		CancelF()
	}
}

func shuttingDown() bool {
	return (atomic.LoadUint32(&Closing) > 0)
}
