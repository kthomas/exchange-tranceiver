package main

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	"github.com/kthomas/go-amqputil"
)

func GdaxFactory(base string, counter string) *StreamingDataSource {
	currencyPair := fmt.Sprintf("%s-%s", base, counter)
	return &StreamingDataSource{
		DestinationAmqpConfig: &amqputil.AmqpConfig{
			AmqpUrl: os.Getenv("AMQP_URL"),
			AmqpBindingKey: fmt.Sprintf("currency.%s", currencyPair),
			AmqpExchange: "ticker",
			AmqpExchangeType: "topic",
			AmqpExchangeDurable: true,
			AmqpQueue: currencyPair,
		},

		Stream: func(ch chan *[]byte) error {
			var wsDialer websocket.Dialer
			wsConn, _, err := wsDialer.Dial("wss://ws-feed.gdax.com", nil)
			if err != nil {
				Log.Errorf("Failed to establish websocket connection to ws-feed.gdax.com")
			} else {
				defer wsConn.Close()
				subscribe := map[string]interface{}{
					"type": "subscribe",
					"product_ids": []string{currencyPair},
				}
				if err := wsConn.WriteJSON(subscribe); err != nil {
					Log.Errorf("Failed to write subscribe message to GDAX websocket connection")
				} else {
					Log.Debugf("Subscribed to %s GDAX websocket", currencyPair)

					for {
						_, message, err := wsConn.ReadMessage()
						if err != nil {
							Log.Errorf("Failed to receive message on GDAX websocket; %s", err)
							break
						} else {
							Log.Debugf("Received message on GDAX websocket: %s", message)
							ch <-&message
						}
					}
				}
			}
			return err
		},
	}
}

func OandaFactory(base string, counter string) *StreamingDataSource {
	currencyPair := fmt.Sprintf("%s-%s", base, counter)
	return &StreamingDataSource{
		DestinationAmqpConfig: &amqputil.AmqpConfig{
			AmqpUrl: os.Getenv("AMQP_URL"),
			AmqpBindingKey: fmt.Sprintf("currency.%s", currencyPair),
			AmqpExchange: "ticker",
			AmqpExchangeType: "topic",
			AmqpExchangeDurable: true,
			AmqpQueue: currencyPair,
		},

		Stream: func(ch chan *[]byte) error {
			var accountId string
			var accessToken string
			if os.Getenv("OANDA_API_ACCOUNT_ID") != "" {
				accountId = os.Getenv("OANDA_API_ACCOUNT_ID")
			}
			if os.Getenv("OANDA_API_ACCESS_TOKEN") != "" {
				accessToken = os.Getenv("OANDA_API_ACCESS_TOKEN")
			}
			if accountId == "" || accessToken == "" {
				Log.Warningf("OANDA_API_ACCOUNT_ID and/or OANDA_API_ACCESS_TOKEN invalid for currency pair %s/%s", base, counter)
				return errors.New("OANDA_API_ACCOUNT_ID and/or OANDA_API_ACCESS_TOKEN invalid")
			}

			client := &http.Client{}
			queryString := fmt.Sprintf("instruments=%s_%s", base, counter)
			url := fmt.Sprintf("https://stream-fxtrade.oanda.com/v3/accounts/%s/pricing/stream?%s", accountId, queryString)
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				Log.Errorf("Failed to create request for streaming from OANDA stream-fxtrade.oanda.com")
			} else {
				req.Header.Set("authorization", fmt.Sprintf("bearer %s", accessToken))
				res, err := client.Do(req)
				if err != nil {
					Log.Errorf("Failed to establish connection to OANDA stream-fxtrade.oanda.com")
				} else {
					defer res.Body.Close()
					reader := bufio.NewReader(res.Body)

					for {
						message, err := reader.ReadBytes('\n')
						if err != nil {
							Log.Errorf("Failed to receive message from OANDA stream; %s", err)
							break
						} else {
							Log.Debugf("Received message from OANDA stream: %s", message)
							ch <-&message
						}
					}
				}
			}
			return err
		},
	}
}
