package interbroker

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
)

type ClientMap map[int]*clientInfo

func (g *gossiper) OpenConnections() error {
	// Open connections in the background
	go func() {
		g.connectionsMutex.Lock()
		defer g.connectionsMutex.Unlock()
		peers := g.discoverer.Peers()
		var m ClientMap
		m = make(ClientMap, len(peers))
		for _, peer := range peers {
			m[peer.Ordinal] = g.createClient()
		}
		g.connections.Store(m)
	}()

	return nil
}

func (g *gossiper) createClient() *clientInfo {
	transport := &http2.Transport{
		AllowHTTP: true,
		// Pretend we are dialing a TLS endpoint
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			log.Info().Msgf("Connecting to %s", addr)
			return net.Dial(network, addr)
		},
		ReadIdleTimeout: 400 * time.Millisecond,
		PingTimeout:     600 * time.Millisecond,
	}

	client := &http.Client{
		Transport: transport,
	}

	return &clientInfo{client, transport}
}

type clientInfo struct {
	client    *http.Client
	transport *http2.Transport
}
