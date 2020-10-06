package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-kit/kit/endpoint"
	"github.com/objenious/kitty"
)

const url = "mqtt://test"

func makeTestEP(resChan chan *testStruct) endpoint.Endpoint {
	return func(_ context.Context, req interface{}) (interface{}, error) {
		if r, ok := req.(*testStruct); ok {
			resChan <- r
			return nil, nil
		}
		return nil, errors.New("invalid format")
	}
}

func makeTransport(ctx context.Context, errChan chan error) *Transport {
	return NewTransport(ctx, url).Middleware(func(h Handler) Handler {
		return func(clt MQTT.Client, msg MQTT.Message) error {
			err := h(clt, msg)
			if err != nil {
				errChan <- err
			}
			return err
		}
	})
}

// to launch before : gcloud beta emulators pubsub start
func TestSingleEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	createTopicAndSub(ctx, "pub", "", "")

	resChan := make(chan *testStruct)
	errChan := make(chan error)
	tr := makeTransport(ctx, errChan).Endpoint("sub", makeTestEP(resChan), Decoder(decode))
	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		send(ctx, tr, "pub", 0, true, `{"foo":"bar"}`)

		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case res := <-resChan:
			if res.Foo != "bar" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestSynchronous(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	createTopicAndSub(ctx, "syncpub", "", "")

	resChan := make(chan *testStruct)
	errChan := make(chan error)
	tr := makeTransport(ctx, errChan).Endpoint("syncsub", makeTestEP(resChan), Decoder(decode))
	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		send(ctx, tr, "syncpub", 0, false, `{"foo":"bar"}`)

		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case res := <-resChan:
			if res.Foo != "bar" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestMultipleEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	createTopicAndSub(ctx, "mpub", "", "")
	createTopicAndSub(ctx, "mpub2", "", "")
	errChan := make(chan error)
	resChan := make(chan *testStruct)
	res2Chan := make(chan *testStruct)
	tr := makeTransport(ctx, errChan).
		Endpoint("msub", makeTestEP(resChan), Decoder(decode)).
		Endpoint("msub2", makeTestEP(res2Chan), Decoder(decode))

	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		err := send(ctx, tr, "mpub", 0, false, `{"foo":"bar"}`)
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case res := <-resChan:
			if res.Foo != "bar" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		case <-res2Chan:
			t.Error("wrong endpoint called")
		}
	}
	{
		err := send(ctx, tr, "mpub2", 0, false, `{"foo":"bar2"}`)
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case <-resChan:
			t.Error("wrong endpoint called")
		case res := <-res2Chan:
			if res.Foo != "bar2" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	createTopicAndSub(ctx, "epub", "", "")

	errChan := make(chan error)
	tr := makeTransport(ctx, errChan).Endpoint("esub", func(_ context.Context, req interface{}) (interface{}, error) { return nil, errors.New("foo") }, Decoder(decode))
	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		send(ctx, tr, "epub", 0, false, `{"foo":"bar"}`)

		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			if err.Error() != "foo" {
				t.Errorf("endpoint returned an invalid error: %v (should have been an endpoint error)", err)
			}
		}
	}
	{
		send(ctx, tr, "epub", 0, false, `{"foo":1}`)
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			if !strings.HasPrefix(err.Error(), "decode error") {
				t.Errorf("endpoint returned an invalid error: %v (should have been a decode error)", err)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	createTopicAndSub(ctx, "xpubsub", "", "")

	shutdownCalled := false
	exitChan := make(chan error)
	tr := NewTransport(ctx, url).Endpoint("xpubsub", func(_ context.Context, req interface{}) (interface{}, error) { return nil, nil })
	go func() {
		srv := kitty.NewServer(tr).Shutdown(func() {
			shutdownCalled = true
		})
		exitChan <- srv.Run(ctx)
	}()

	cancel()
	select {
	case err := <-exitChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Server.Run returned an error : %s", err)
		}
	}
	if !shutdownCalled {
		t.Error("Shutdown functions are not called")
	}
}

type testStruct struct {
	Foo string `json:"foo"`
}

func decode(clt MQTT.Client, m MQTT.Message) (interface{}, error) {
	d := &testStruct{}
	err := json.Unmarshal(m.Payload(), d)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	return d, nil
}

// send sends a message to Pub/Sub topic. The topic must already exist.
func send(ctx context.Context, tr *Transport, topic string, qos byte, retained bool, payload interface{}) error {
	for tr.clt == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(time.Millisecond)
		}
	}
	token := tr.clt.Publish(topic, qos, retained, payload)
	return token.Error()
}

func createTopicAndSub(ctx context.Context, clientID, username, password string) {
	connOpts := MQTT.NewClientOptions().
		AddBroker(url).
		SetClientID(clientID).
		SetCleanSession(false).
		SetUsername(username).
		SetPassword(password).
		SetAutoReconnect(true).
		SetOrderMatters(false) // if we set that to true, we're loosing concurrency

	c := MQTT.NewClient(connOpts)
	c.Disconnect(0)
}
