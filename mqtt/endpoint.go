package mqtt

import (
	"context"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-kit/kit/endpoint"
)

// EndpointOption is a function to set option in endpoint
type EndpointOption func(*Endpoint)

// DecodeRequestFunc is a function to decode pub/sub message and return structured data
type DecodeRequestFunc func(context.Context, MQTT.Message) (interface{}, error)

// Endpoint for this pubsub transport
type Endpoint struct {
	// argument
	topic string
	// options
	decode DecodeRequestFunc
	// runtime
	endpoint endpoint.Endpoint
}

// Decoder sets the decode function for requests in the endpoint
func Decoder(d DecodeRequestFunc) func(e *Endpoint) {
	return func(e *Endpoint) { e.decode = d }
}
