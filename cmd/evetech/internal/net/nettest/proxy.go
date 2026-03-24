// package nettest
//
// See https://github.com/Shopify/toxiproxy?tab=readme-ov-file#toxics
package nettest

import (
	"github.com/Shopify/toxiproxy/v2"
	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/travisjeffery/go-dynaport"
)

type Client struct {
	c *toxiclient.Client
}

func (c *Client) Bandwidth(proxy string, stream Stream, rate int64) (ok bool, stop func()) {
	toxic, err := c.c.AddToxic(&toxiclient.ToxicOptions{
		ProxyName: proxy,
		ToxicName: "", //? default to <type>_<stream>
		ToxicType: "bandwidth",
		Stream:    stream.String(),
		Attributes: toxiclient.Attributes{
			"rate": rate,
		},
	})
	if err != nil {
		return
	}
	return true, func() { _ = c.c.RemoveToxic(&toxiclient.ToxicOptions{ProxyName: proxy, ToxicName: toxic.Name}) }
}

func (c *Client) Latency(proxy string, stream Stream, latency, jitter float64) (ok bool, stop func()) {
	toxic, err := c.c.AddToxic(&toxiclient.ToxicOptions{
		ProxyName: proxy,
		ToxicName: "", //? default to <type>_<stream>
		ToxicType: "latency",
		Stream:    stream.String(),
		Attributes: toxiclient.Attributes{
			"latency": latency, // milliseconds
			"jitter":  jitter,  // milliseconds
		},
	})
	if err != nil {
		return
	}
	return true, func() { _ = c.c.RemoveToxic(&toxiclient.ToxicOptions{ProxyName: proxy, ToxicName: toxic.Name}) }
}

type Config struct {
	Name     string
	Upstream string
}

func (c *Client) Proxy(name, upstream string) (string, error) {
	ports := dynaport.GetS(1)
	p, err := c.c.CreateProxy(name, ":"+ports[0], upstream)
	if err != nil {
		return "", err
	}
	return p.Listen, nil
}

type Stream bool

const (
	Downstream Stream = false
	Upstream   Stream = true
)

func (s Stream) String() string {
	if s {
		return "upstream"
	}
	return "downstream"
}

type Server struct {
	s *toxiproxy.ApiServer

	client *Client
}

func (s *Server) Client() *Client { return s.client }

func (c *Server) Close() { c.s.Shutdown() }

func NewServer() *Server {
	ports := dynaport.GetS(1)
	addr := ":" + ports[0] // 127.0.0.1
	mc := toxiproxy.NewMetricsContainer(prometheus.NewRegistry())
	s := toxiproxy.NewServer(mc, zerolog.Nop())
	go s.Listen(addr)

	c := &Client{c: toxiclient.NewClient(addr)}
	return &Server{s: s, client: c}
}
