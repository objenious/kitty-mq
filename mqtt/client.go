package mqtt

import (
	"context"
	"errors"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-kit/kit/endpoint"
)

// Client defines the attributes of the client
type Client struct {
	client   MQTT.Client
	topic    string
	qos      byte
	retained bool
	timeout  time.Duration
	encode   EncodeRequestFunc
}

// EncodeRequestFunc defines the encode request function
type EncodeRequestFunc func(interface{}) (interface{}, error)

// NewClient creates the client
func NewClient(ctx context.Context, url, clientID, username, password, topic string, opts ...ClientOption) (*Client, error) {
	connOpts := MQTT.NewClientOptions().
		AddBroker(url).
		SetClientID(clientID).
		SetCleanSession(false).
		SetUsername(username).
		SetPassword(password).
		SetAutoReconnect(true).
		SetOrderMatters(false) // if we set that to true, we're loosing concurrency

	c := MQTT.NewClient(connOpts)
	client := &Client{
		client: c,
		topic:  topic,
		encode: noEncoder,
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// ClientOption sets an optional parameter for clients.
type ClientOption func(*Client)

// EncodeRequest sets the encode request function of the client
func EncodeRequest(er EncodeRequestFunc) ClientOption {
	return func(c *Client) {
		c.encode = er
	}
}

func noEncoder(v interface{}) (interface{}, error) {
	return v, nil
}

// SetClient sets the pubsub client
func SetClient(client MQTT.Client) ClientOption {
	return func(c *Client) {
		c.client = client
	}
}

// do sends the message
func (c *Client) do(ctx context.Context, m interface{}) error {
	data, err := c.encode(m)
	if err != nil {
		return err
	}
	token := c.client.Publish(c.topic, c.qos, c.retained, data)
	err = token.Error()
	if err != nil {
		return err
	}
	if !token.WaitTimeout(c.timeout) {
		return errors.New("publish time out")
	}
	return nil
}

// Close closes the client
func (c *Client) Close() error {
	c.client.Disconnect(0)
	return nil
}

// Endpoint creates an endpoint
func (c *Client) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, r interface{}) (interface{}, error) {
		return nil, c.do(ctx, r)
	}
}
