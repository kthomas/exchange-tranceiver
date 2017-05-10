package main

import (
	"fmt"

	"github.com/gorilla/websocket"
)

func GdaxFactory(base string, counter string) *StreamingDataSource {
	currencyPair := fmt.Sprintf("%s-%s", base, counter)
	return &StreamingDataSource{
		AmqpBindingKey: fmt.Sprintf("currency.%s", currencyPair),
		AmqpExchange: "ticker",
		AmqpExchangeType: "topic",
		AmqpExchangeDurable: true,
		AmqpQueue: currencyPair,

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
