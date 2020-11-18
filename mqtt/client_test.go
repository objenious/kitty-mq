package mqtt

import (
	"context"
	"fmt"
	"testing"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func TestClient(t *testing.T) {
	t.Log("mqtt setup")
	ctx := context.TODO()
	connOpts := MQTT.NewClientOptions().
		AddBroker(url).
		SetClientID("clientID").
		SetCleanSession(false).
		SetUsername("username").
		SetPassword("password").
		SetAutoReconnect(true).
		SetOrderMatters(false) // if we set that to true, we're loosing concurrency

	mqttClt := MQTT.NewClient(connOpts)

	ch := make(chan MQTT.Message, 1)
	sub := mqttClt.Subscribe("testclientsub", 1, func(clt MQTT.Client, msg MQTT.Message) {
		ch <- msg
		msg.Ack()
	})
	if err := sub.Error(); err != nil {
		t.Fatal("can't subscribe", err)
	}

	t.Log("create client")

	c, err := NewClient(ctx, "url", "clientID", "username", "password", "topic")
	if err != nil {
		t.Fatal("new client under test", err)
	}

	endp := c.Endpoint()

	t.Log("send data with the client, test reception")
	timer := time.NewTimer(10 * time.Second)
	for i := 0; i < 10; i++ {
		_, err = endp(ctx, fmt.Sprintf("hello world! #%d", i))
		if err != nil {
			t.Error("client endpoint", err)
		}

		timer.Reset(10 * time.Second)
		select {
		case m := <-ch:
			if m == nil {
				t.Error("no message found", err)
			}
			if fmt.Sprintf(`"hello world! #%d"`, i) != string(m.Payload()) {
				t.Error("message expected is different", err)
			}
		case <-timer.C:
			t.Error("timeout reached")
		}
	}
	timer.Stop()

	t.Log("mqtt clean up")
	mqttClt.Disconnect(1)
}
