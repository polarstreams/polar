package interbroker

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
)

type ClientMap map[int]*clientInfo

func (g *gossiper) OpenConnections() error {
	// Open connections in the background
	c := make(chan bool, 1)
	go func() {
		c <- true
		// We could use a single client for all peers but to
		// reduce contention and having more fine grained control, we use one per each peer
		g.connectionsMutex.Lock()
		defer g.connectionsMutex.Unlock()
		peers := g.discoverer.Peers()
		m := make(ClientMap, len(peers))
		var wg sync.WaitGroup
		for _, peer := range peers {
			wg.Add(1)
			clientInfo := g.createClient()
			log.Debug().Msgf("Before first connection, is up? %v", clientInfo.isHostUp())
			m[peer.Ordinal] = clientInfo
			go func(p *types.BrokerInfo) {
				_, err := clientInfo.client.Get(g.GetPeerUrl(p, conf.StatusUrl))

				wg.Done()
				if err != nil {
					// TODO: Continue reconnection in the background
					log.Err(err).Msgf("Initial connection to peer %s failed", p)
				} else {
					log.Debug().Msgf("Connected to peer %s", p)
				}
			}(&peer)
		}
		wg.Wait()

		g.connections.Store(m)
	}()

	<-c
	log.Info().Msg("Start opening connections to peers")

	return nil
}

func (g *gossiper) createClient() *clientInfo {
	var connection atomic.Value

	transport := &http2.Transport{
		AllowHTTP: true,
		// Pretend we are dialing a TLS endpoint
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			log.Debug().Msgf("Creating connection to %s", addr)
			conn, err := net.Dial(network, addr)
			if err != nil {
				// Clean whatever is in cache with a connection marked as closed
				connection.Store(newFailedConnection())
				return conn, err
			}

			c := newOpenConnection(conn)
			connection.Store(c)
			return c, nil
		},
		ReadIdleTimeout: 400 * time.Millisecond,
		PingTimeout:     600 * time.Millisecond,
	}

	client := &http.Client{
		Transport: transport,
	}

	return &clientInfo{client, transport, &connection}
}

func (g *gossiper) GetPeerUrl(b *types.BrokerInfo, path string) string {
	return fmt.Sprintf("http://%s:%d%s", b.HostName, g.config.GossipPort(), path)
}

type clientInfo struct {
	client     *http.Client
	transport  *http2.Transport
	connection *atomic.Value
}

// isHostUp determines whether a host is considered UP
func (cli *clientInfo) isHostUp() bool {
	c, ok := cli.connection.Load().(*connectionWrapper)
	return ok && c.isOpen
}
