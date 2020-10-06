package mqtt

import (
	"context"
	"errors"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-kit/kit/endpoint"
	"github.com/objenious/kitty"
	"golang.org/x/sync/errgroup"
)

// Transport is a transport that receives requests from PubSub
type Transport struct {
	url          string
	clientID     string
	username     string
	password     string
	qos          byte
	cleanSession bool
	reconnect    bool
	orderMatters bool
	clt          MQTT.Client
	middleware   Middleware
	endpoints    []*Endpoint
}

var _ kitty.Transport = &Transport{}

// NewTransport creates a new Transport for the related Google Cloud Project
func NewTransport(ctx context.Context, url string) *Transport {
	return &Transport{
		url:        url,
		middleware: nopMiddleWare,
	}
}

// Endpoint creates a new Endpoint
func (t *Transport) Endpoint(topic string, endpoint endpoint.Endpoint, options ...EndpointOption) *Transport {
	e := &Endpoint{
		topic:    topic,
		endpoint: endpoint,
	}
	for _, opt := range options {
		opt(e)
	}
	t.endpoints = append(t.endpoints, e)
	return t
}

// Middleware sets a Pub/Sub middleware for all endpoint handlers
func (t *Transport) Middleware(m Middleware) *Transport {
	t.middleware = m
	return t
}

// Start starts listening to PubSub
func (t *Transport) Start(ctx context.Context) error {
	connOpts := MQTT.NewClientOptions().
		AddBroker(t.url).
		SetClientID(t.clientID).
		SetCleanSession(t.cleanSession).
		SetUsername(t.username).
		SetPassword(t.password).
		SetAutoReconnect(t.reconnect).
		SetOrderMatters(t.orderMatters) // if we set that to true, we're loosing concurrency

	t.clt = MQTT.NewClient(connOpts)
	var g errgroup.Group
	for _, e := range t.endpoints {
		endpoint := e
		g.Go(func() error {
			return t.consume(ctx, endpoint)
		})
	}
	return g.Wait()
}

func (t *Transport) consume(ctx context.Context, e *Endpoint) error {
	token := t.clt.Subscribe(e.topic, e.qos, t.makeReceiveFunc(e))
	err := token.Error()
	if err != nil {
		return err
	}
	if e.timeout > time.Second {
		b := token.WaitTimeout(e.timeout)
		if !b {
			return errors.New("wait timeout returns false")
		}
	} else {
		b := token.Wait()
		if !b {
			return errors.New("wait timeout returns false")
		}
	}
	return nil
}

func (t *Transport) makeReceiveFunc(e *Endpoint) MQTT.MessageHandler {
	handler := func(clt MQTT.Client, msg MQTT.Message) error {
		ctx := context.Background()
		PopulateRequestContext(ctx, clt, msg)
		var (
			dec interface{}
			err error
		)
		if e.decode != nil {
			dec, err = e.decode(clt, msg)
		} else {
			dec = msg.Payload()
		}
		if err == nil {
			_, err = e.endpoint(ctx, dec)
		}
		return err
	}
	handler = t.middleware(handler)
	return func(clt MQTT.Client, msg MQTT.Message) {
		defer func() {
			if r := recover(); r != nil {
				msg.Duplicate()
			}
		}()
		err := handler(clt, msg)
		if kitty.IsRetryable(err) {
			msg.Duplicate()
		} else {
			msg.Ack()
		}
	}
}

// RegisterEndpoints registers a middleware to all registered endpoints at that time
func (t *Transport) RegisterEndpoints(m endpoint.Middleware) error {
	for _, e := range t.endpoints {
		e.endpoint = m(e.endpoint)
	}
	return nil
}

// Shutdown shutdowns the google pubsub client
func (t *Transport) Shutdown(ctx context.Context) error {
	if t.clt != nil {
		t.clt.Disconnect(1)
	}
	return nil
}

var logKeys = map[string]interface{}{
	"pubsub-id": contextKeyID,
}

// LogKeys returns the keys for logging
func (*Transport) LogKeys() map[string]interface{} {
	return logKeys
}

// PopulateRequestContext is a RequestFunc that populates several values into
// the context from the pub/sub message. Those values may be extracted using the
// corresponding ContextKey type in this package.
func PopulateRequestContext(ctx context.Context, clt MQTT.Client, msg MQTT.Message) context.Context {
	for k, v := range map[contextKey]uint16{
		contextKeyID: msg.MessageID(),
	} {
		ctx = context.WithValue(ctx, k, v)
	}
	return ctx
}

type contextKey int

const (
	contextKeyID contextKey = iota
)
