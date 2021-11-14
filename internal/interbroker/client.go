package interbroker

import (
	"crypto/tls"
	"fmt"
	"math"
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

const (
	baseReconnectionDelay = 20
	maxReconnectionDelay  = 10_000
)

type clientMap map[int]*clientInfo

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
		m := make(clientMap, len(peers))
		log.Debug().Msgf("Connecting to peers %v", peers)
		var wg sync.WaitGroup
		for _, peer := range peers {
			wg.Add(1)
			c := g.createClient(peer)
			m[peer.Ordinal] = c
			p := peer
			go func() {
				defer wg.Done()
				log.Debug().Msgf("Creating initial peer request to %s", p.HostName)
				_, err := c.client.Get(g.getPeerUrl(&p, conf.StatusUrl))

				if err != nil {
					// Reconnection will continue in the background as part of transport logic
					log.Err(err).Msgf("Initial connection to http peer %s failed", p.HostName)
				}
			}()
		}
		wg.Wait()

		g.connections.Store(m)
	}()

	<-c
	log.Info().Msg("Start opening connections to peers")

	return nil
}

func (g *gossiper) createClient(broker types.BrokerInfo) *clientInfo {
	var connection atomic.Value

	clientInfo := &clientInfo{
		connection:   &connection,
		dataConn:     &atomic.Value{},
		hostName:     broker.HostName,
		dataMessages: make(chan *dataRequest, 256),
	}

	clientInfo.client = &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			// Pretend we are dialing a TLS endpoint
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				// When in-flight streams is below max, there's a single open connection
				log.Debug().Msgf("Creating connection to %s", addr)
				conn, err := net.Dial(network, addr)
				if err != nil {
					// Clean whatever is in cache with a connection marked as closed
					connection.Store(types.NewFailedConnection())
					clientInfo.startReconnection(g, &broker)
					return conn, err
				}

				log.Info().Msgf("Connected to peer %s", addr)
				c := types.NewTrackedConnection(conn, func(c *types.TrackedConnection) {
					clientInfo.startReconnection(g, &broker)
				})

				// Store it at clientInfo level to retrieve the connection status later
				connection.Store(c)
				return c, nil
			},
			// Use an eager health check setting at the cost of a few bytes/sec
			ReadIdleTimeout: 200 * time.Millisecond,
			PingTimeout:     400 * time.Millisecond,
		},
	}

	go clientInfo.openDataConnection(g.config)

	return clientInfo
}

func (g *gossiper) getPeerUrl(b *types.BrokerInfo, path string) string {
	return fmt.Sprintf("http://%s:%d%s", b.HostName, g.config.GossipPort(), path)
}

func (g *gossiper) getClientInfo(ordinal int) *clientInfo {
	if m, ok := g.connections.Load().(clientMap); ok {
		if clientInfo, ok := m[ordinal]; ok {
			return clientInfo
		}
	}

	return nil
}

type clientInfo struct {
	client         *http.Client
	connection     *atomic.Value
	dataConn       *atomic.Value
	dataMessages   chan *dataRequest
	hostName       string
	isReconnecting int32
}

func (c *clientInfo) openDataConnection(config conf.GossipConfig) {
	i := 0
	for {
		dataConn, err := newDataConnection(c, config)
		if err != nil {
			delay := math.Pow(2, float64(i)) * baseReconnectionDelay
			if delay > maxReconnectionDelay {
				delay = maxReconnectionDelay
			} else {
				i++
			}
			log.Info().Msgf("Client data connection to %s could not be opened, retrying", c.hostName)
			// TODO: Add jitter
			time.Sleep(time.Duration(delay) * time.Millisecond)
			continue
		}

		// Reset delay
		i = 0

		// Wait for connection to be closed
		<-dataConn.closed
	}
}

// isHostUp determines whether a host is considered UP
func (cli *clientInfo) isHostUp() bool {
	c, ok := cli.connection.Load().(*types.TrackedConnection)
	return ok && c.IsOpen()
}

// startReconnection starts reconnection in the background if it hasn't started
func (c *clientInfo) startReconnection(g *gossiper, broker *types.BrokerInfo) {
	// Determine is already reconnecting
	if !atomic.CompareAndSwapInt32(&c.isReconnecting, 0, 1) {
		return
	}

	log.Info().Msgf("Start reconnecting to %s", broker.HostName)

	go func() {
		i := 0
		for {
			delay := math.Pow(2, float64(i)) * baseReconnectionDelay
			if delay > maxReconnectionDelay {
				delay = maxReconnectionDelay
			} else {
				i++
			}
			// TODO: Add jitter
			time.Sleep(time.Duration(delay) * time.Millisecond)

			if c := g.getClientInfo(broker.Ordinal); c == nil || c.hostName != broker.HostName {
				// Topology changed, stop reconnecting
				break
			}

			log.Debug().Msgf("Attempting to reconnect to %s after %v ms", broker, delay)

			response, err := c.client.Get(g.getPeerUrl(broker, conf.StatusUrl))
			if err == nil && response.StatusCode == http.StatusOK {
				// Succeeded
				break
			}
		}

		// Leave field as 0 to allow new reconnections
		atomic.StoreInt32(&c.isReconnecting, 0)
	}()
}
