package interbroker

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
)

const (
	baseReconnectionDelayMs = 20
	maxReconnectionDelayMs  = 10_000
	removeClientCheckDelay  = 5 * time.Second
)

func (g *gossiper) OpenConnections() {
	topology := g.discoverer.Topology()
	log.Info().Msgf("Start opening connections to %d peers", len(topology.Brokers)-1)
	g.createNewClients(g.discoverer.Topology())
}

// Creates the new clients, without replacing the existing ones, and starts connecting to the peers
func (g *gossiper) createNewClients(topology *TopologyInfo) {
	// We could use a single client for all peers but to
	// reduce contention and having fine grained control, we use one per each peer
	peers := topology.Peers()
	g.connectionsMutex.Lock()
	defer g.connectionsMutex.Unlock()

	var m clientMap
	existing := g.connections.Load()
	if existing == nil {
		m = make(clientMap, len(peers))
	} else {
		m = existing.(clientMap).clone()
	}

	var wg sync.WaitGroup
	for _, peer := range peers {
		if _, ok := m[peer.Ordinal]; ok {
			// Avoid replacing the existing ones
			continue
		}
		wg.Add(1)
		c := g.createClient(peer)
		m[peer.Ordinal] = c

		// Captured in closure
		broker := peer
		go func() {
			defer wg.Done()
			log.Debug().Msgf("Creating initial peer request to %s", broker.HostName)
			err := c.makeFirstRequest(g, &broker)

			if err != nil {
				// Reconnection will continue in the background as part of transport logic
				log.Err(err).Msgf("Initial connection to http peer %s failed", broker.HostName)
			}
		}()
	}
	wg.Wait()

	g.connections.Store(m)
}

// Mark the clients pointing to the brokers that are leaving
func (g *gossiper) markAsLeaving(previousTopology *TopologyInfo, topology *TopologyInfo) {
	ordinals := make([]int, 0)
	for i := len(topology.Brokers); i < len(previousTopology.Brokers); i++ {
		ordinals = append(ordinals, i)
	}
	log.Debug().Msgf("Marking brokers with ordinals %v as leaving", ordinals)

	for _, ordinal := range ordinals {
		c := g.getClientInfo(ordinal)
		if c == nil {
			log.Error().Msgf("Client for broker with ordinal %d not found", ordinal)
			continue
		}

		c.setAsLeaving()
	}
}

// Waits on a different goroutine for hosts to be considered as down and then removes the clients
func (g *gossiper) waitForDownAndRemoveClients(previousTopology *TopologyInfo, topology *TopologyInfo) {
	ordinals := make([]int, 0)
	for i := len(topology.Brokers); i < len(previousTopology.Brokers); i++ {
		ordinals = append(ordinals, i)
	}

	go func() {
		allHostAreDown := false
		for !allHostAreDown {
			log.Debug().Msgf("Waiting for brokers %v to be down before removing them", ordinals)
			time.Sleep(removeClientCheckDelay)

			for _, ordinal := range ordinals {
				c := g.getClientInfo(ordinal)
				isHostUp := c != nil && c.isHostUp()
				allHostAreDown = !isHostUp

				if isHostUp {
					log.Debug().Msgf("B%d is still considered UP", ordinal)
					break
				}
			}
		}

		log.Info().Msgf("Removing brokers with ordinals %v", ordinals)

		g.connectionsMutex.Lock()
		defer g.connectionsMutex.Unlock()

		m := g.connections.Load().(clientMap).clone()
		for _, ordinal := range ordinals {
			c, ok := m[ordinal]
			if !ok {
				log.Error().Msgf("Client for B%d not found", ordinal)
				continue
			}

			if !c.isHostLeaving() {
				log.Error().Msgf("Race condition when removing client info for B%d", ordinal)
				continue
			}

			delete(m, ordinal)

			c.gossipClient.CloseIdleConnections()
			c.routingClient.CloseIdleConnections()
			if dataConnValue := c.dataConn.Load(); dataConnValue != nil {
				dataConn := dataConnValue.(*dataConnection)
				dataConn.close()
			}
		}
		g.connections.Store(m)
		log.Info().Msgf("Gossip now contains %d clients for %d peers", len(m), len(topology.Brokers)-1)
	}()
}

func (g *gossiper) onHostDown(b *BrokerInfo) {
	if g.localDb.IsShuttingDown() {
		return
	}
	for _, listener := range g.hostUpDownListeners {
		listener.OnHostDown(*b)
	}
}

func (g *gossiper) onHostUp(b *BrokerInfo) {
	if g.localDb.IsShuttingDown() {
		return
	}
	for _, listener := range g.hostUpDownListeners {
		listener.OnHostUp(*b)
	}
}

func (g *gossiper) createClient(broker BrokerInfo) *clientInfo {
	gossipConnection := &atomic.Value{}

	clientInfo := &clientInfo{
		gossipConnection:         gossipConnection,
		dataConn:                 &atomic.Value{},
		hostName:                 broker.HostName,
		isConnected:              0,
		dataMessages:             make(chan *dataRequest, 256),
		onHostDown:               g.onHostDown,
		onHostUp:                 g.onHostUp,
		readyNewDataConnection:   make(chan bool, 2),
		readyNewGossipConnection: make(chan bool, 2),
	}

	clientInfo.gossipClient = &http.Client{
		Transport: &http2.Transport{
			StrictMaxConcurrentStreams: true, // Do not create additional connections
			AllowHTTP:                  true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				// Pretend we are dialing a TLS endpoint.

				// When in-flight streams is below max, there's a single open connection
				log.Debug().Msgf("Creating gossip connection to %s", addr)
				conn, err := net.Dial(network, addr)
				if err != nil {
					// Clean whatever is in cache with a connection marked as closed
					gossipConnection.Store(NewFailedConnection())
					clientInfo.startReconnection(g, &broker)
					return conn, err
				}

				// Set as connected
				atomic.StoreInt32(&clientInfo.isConnected, 1)

				log.Info().Msgf("Connected to peer %s on gossip port", addr)
				c := NewTrackedConnection(conn, func(c *TrackedConnection) {
					log.Warn().Msgf("Connection to peer %s on gossip port closed", addr)
					clientInfo.startReconnection(g, &broker)
				})

				// Store it at clientInfo level to retrieve the connection status later
				// TODO: Unused, maybe remove
				gossipConnection.Store(c)
				return c, nil
			},
			// Use an eager health check setting at the cost of a few bytes/sec
			ReadIdleTimeout: 200 * time.Millisecond,
			PingTimeout:     400 * time.Millisecond,
		},
	}

	clientInfo.routingClient = &http.Client{
		Transport: &http2.Transport{
			StrictMaxConcurrentStreams: true, // Do not create additional connections
			AllowHTTP:                  true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				// Pretend we are dialing a TLS endpoint
				// When in-flight streams is below max, there's a single open connection
				log.Debug().Msgf("Creating peer connection to %s for re-routing", addr)
				conn, err := net.Dial(network, addr)
				if err != nil {
					return nil, err
				}

				log.Info().Msgf("Connected to peer %s on re-routing (gossip) port", addr)
				c := NewTrackedConnection(conn, func(c *TrackedConnection) {
					log.Warn().Msgf("Connection to peer %s on re-routing port closed", addr)
				})
				return c, nil
			},
			// A little less eager than gossip health checks
			ReadIdleTimeout: 1 * time.Second,
			PingTimeout:     2 * time.Second,
		},
	}

	go clientInfo.openDataConnection(g.config)

	return clientInfo
}

func (g *gossiper) getPeerUrl(b *BrokerInfo, path string) string {
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
	gossipClient             *http.Client  // HTTP/2 client for gossip messages
	routingClient            *http.Client  // HTTP/2 client for re-routing events to the natural leader
	gossipConnection         *atomic.Value // Tracked connection to determine gossip state
	dataConn                 *atomic.Value // Client data connection
	isConnected              int32         // Determines whether there's an open connection to gossip HTTP/2 server
	dataMessages             chan *dataRequest
	hostName                 string
	isReconnecting           int32             // Used for concurrency control on reconnection
	isLeaving                int32             // Determines whether the host is leaving the cluster
	onHostDown               func(*BrokerInfo) // Func to be called when broker status changed from up to down
	onHostUp                 func(*BrokerInfo) // Func to be called when broker status changed from down to up
	readyNewDataConnection   chan bool         // Gets a message when the peer is ready to accept data connections
	readyNewGossipConnection chan bool         // Gets a message when the peer is ready to accept gossip connections
}

func (c *clientInfo) openDataConnection(config conf.GossipConfig) {
	i := 0
	shouldExit := false
	for !shouldExit || !c.isHostLeaving() {
		dataConn, err := newDataConnection(c, config)
		if err != nil {
			delayMs := math.Pow(2, float64(i)) * baseReconnectionDelayMs
			if delayMs > maxReconnectionDelayMs {
				delayMs = maxReconnectionDelayMs
			} else {
				i++
			}

			log.Info().Msgf("Client gossip data connection to %s could not be opened, retrying", c.hostName)
			delay := utils.Jitter(time.Duration(delayMs) * time.Millisecond)

			select {
			case shouldConnect := <-c.readyNewDataConnection:
				if !shouldConnect {
					shouldExit = true
					log.Info().Msgf("Not attempting further data reconnections to %s", c.hostName)
				} else {
					log.Debug().Msgf("Attempting new data connection after receiving ready message")
				}
			case <-time.After(delay):
			}

			continue
		}

		// Reset delay
		i = 0
		c.dataConn.Store(dataConn)

		// Wait for connection to be closed
		<-dataConn.closed
	}
}

// isHostUp determines whether a host is considered UP
func (cli *clientInfo) isHostUp() bool {
	return atomic.LoadInt32(&cli.isConnected) == 1
}

// startReconnection starts reconnection in the background if it hasn't started
func (c *clientInfo) startReconnection(g *gossiper, broker *BrokerInfo) {
	// Dial handlers might call this func multiple times
	if !atomic.CompareAndSwapInt32(&c.isReconnecting, 0, 1) {
		return
	}

	// Set as not connected
	if atomic.CompareAndSwapInt32(&c.isConnected, 1, 0) {
		if !c.isHostLeaving() {
			log.Warn().Msgf("Broker %s considered DOWN", broker.HostName)
			c.onHostDown(broker)
		}
	}

	if c.isHostLeaving() {
		log.Info().Msgf("Not starting reconnection flow as %s is leaving", broker.HostName)
		return
	}

	log.Info().Msgf("Start reconnecting to %s", broker.HostName)

	go func() {
		i := 0
		succeeded := false
		for !c.isHostLeaving() {
			delayMs := math.Pow(2, float64(i)) * baseReconnectionDelayMs
			if delayMs > maxReconnectionDelayMs {
				delayMs = maxReconnectionDelayMs
			} else {
				i++
			}

			delay := utils.Jitter(time.Duration(delayMs) * time.Millisecond)
			shouldConnect := true

			select {
			case shouldConnect = <-c.readyNewGossipConnection:
				if !shouldConnect {
					break
				}
				log.Debug().Msgf("Attempting to reconnect to %s after receiving a ready message", broker)
			case <-time.After(delay):
				log.Debug().Msgf("Attempting to reconnect to %s after %v ms", broker, delayMs)
			}

			if !shouldConnect {
				log.Debug().Msgf("Not attempting further gossip reconnections to %s", c.hostName)
				break
			}

			err := c.makeFirstRequest(g, broker)
			if err == nil {
				succeeded = true
				break
			}
		}

		if succeeded && atomic.CompareAndSwapInt32(&c.isConnected, 0, 1) {
			log.Info().Msgf("Broker %s considered UP", broker.HostName)
			c.onHostUp(broker)
		}

		// Leave field as 0 to allow new reconnections
		atomic.StoreInt32(&c.isReconnecting, 0)
	}()
}

// Makes the first request as a gossip client and returns nil when succeeded
func (c *clientInfo) makeFirstRequest(g *gossiper, broker *BrokerInfo) error {
	ordinal := g.discoverer.Topology().MyOrdinal()
	// Serialization can't fail
	jsonBody, _ := json.Marshal(ordinal)

	resp, err := c.gossipClient.Post(
		g.getPeerUrl(broker, conf.GossipBrokerIdentifyUrl), contentType, bytes.NewReader(jsonBody))

	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := utils.ReadBodyClose(resp)
		log.Error().Msgf("Initial gossip response from %s with status not OK: %s", broker, body)
		return fmt.Errorf("Initial request to %s failed: %s", broker, body)
	}
	return nil
}

func (c *clientInfo) setAsLeaving() {
	atomic.StoreInt32(&c.isLeaving, 1)
	c.readyNewDataConnection <- false
	c.readyNewGossipConnection <- false
}

func (c *clientInfo) isHostLeaving() bool {
	return atomic.LoadInt32(&c.isLeaving) == 1
}
