// package nettest
//
// See https://github.com/Shopify/toxiproxy?tab=readme-ov-file#toxics
package netproxy

import (
	server "github.com/Shopify/toxiproxy/v2"
	client "github.com/Shopify/toxiproxy/v2/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/travisjeffery/go-dynaport"
)

type Proxy struct{ p *client.Proxy }

func (p *Proxy) Listen() string { return p.p.Listen }

func (p *Proxy) Bandwidth(stream Stream, rate int64) (ok bool, stop func()) {
	attr := &client.Attributes{
		"rate": rate, // mbps
	}
	t, err := p.p.AddToxic("", "bandwidth", stream.String(), 1.0, *attr)
	return err == nil, func() { _ = p.p.RemoveToxic(t.Name) }
}

func (p *Proxy) Latency(stream Stream, latency, jitter float64) (ok bool, stop func()) {
	attr := &client.Attributes{
		"latency": latency, // milliseconds
		"jitter":  jitter,  // milliseconds
	}
	t, err := p.p.AddToxic("", "latency", stream.String(), 1.0, *attr)
	return err == nil, func() { _ = p.p.RemoveToxic(t.Name) }
}

type Client struct {
	c *client.Client
}

func (c *Client) Proxy(name, upstream string) (*Proxy, error) {
	ports := dynaport.GetS(1)
	p, err := c.c.CreateProxy(name, ":"+ports[0], upstream)
	if err != nil {
		return nil, err
	}
	return &Proxy{p}, nil
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
	s *server.ApiServer
	c *client.Client
}

func (s *Server) Client() *Client { return &Client{s.c} }

func (c *Server) Close() { c.s.Shutdown() }

func NewServer() *Server {
	ports := dynaport.GetS(1)
	addr := ":" + ports[0] // 127.0.0.1
	mc := server.NewMetricsContainer(prometheus.NewRegistry())
	s := server.NewServer(mc, zerolog.Nop())
	go s.Listen(addr)

	return &Server{s: s, c: client.NewClient(addr)}
}
