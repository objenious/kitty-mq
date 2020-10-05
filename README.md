# kitty-mq

<!--[![Travis-CI](https://travis-ci.org/objenious/kitty-gcp.svg?branch=master)](https://travis-ci.org/objenious/kitty-gcp)  [![GoDoc](https://godoc.org/github.com/objenious/kitty-gcp?status.svg)](http://godoc.org/github.com/objenious/kitty-gcp)
[![GoReportCard](https://goreportcard.com/badge/github.com/objenious/kitty-gcp)](https://goreportcard.com/report/github.com/objenious/kitty-gcp)
[![Coverage Status](https://coveralls.io/repos/github/objenious/kitty-gcp/badge.svg?branch=master)](https://coveralls.io/github/objenious/kitty-gcp?branch=master)-->

`go get github.com/objenious/kitty-mq`

## Status: alpha - breaking changes might happen

kitty-mq adds support for any Message Queue to [kitty](https://github.com/objenious/kitty).

## MQTT

Connect to MQTT :
```
tr := mqtt.NewTransport(ctx, "URL").
  Endpoint(topicName, endpoint, Decoder(decodeFunc))
err := kitty.NewServer(tr).Run(ctx)
```

If your service needs to run on Kubernetes, you also need to have a http transport for keep-alives :
```
tr := mqtt.NewTransport(ctx, "URL").
  Endpoint(topicName, endpoint, Decoder(decodeFunc))
h := kitty.NewHTTPTransport(kitty.Config{HTTPPort: 8080})
err := kitty.NewServer(tr, h).Run(ctx)
```