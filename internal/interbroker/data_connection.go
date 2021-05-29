package interbroker

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/jorgebay/soda/internal/conf"
)

const maxStreamIds = 512

type dataConnection struct {
	closed    chan bool
	streamIds chan streamId
	cli       *clientInfo
	handlers  sync.Map
}

func newDataConnection(cli *clientInfo, config conf.GossipConfig) (*dataConnection, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", cli.hostName, config.GossipDataPort()))
	if err != nil {
		return nil, err
	}

	var once sync.Once
	closed := make(chan bool)
	closeHandler := func() {
		once.Do(func() {
			conn.Close()
			closed <- true
			// TODO: Respond to all sent requests with connection error
		})
	}

	streamIds := make(chan streamId, maxStreamIds)
	for i := 0; i < maxStreamIds; i++ {
		streamIds <- streamId(i)
	}

	c := &dataConnection{
		closed:    closed,
		streamIds: streamIds,
		cli:       cli,
		handlers:  sync.Map{},
	}

	go c.readDataResponses(conn, config, closeHandler)
	go c.writeDataRequests(conn, config, closeHandler)

	return c, nil
}

func (c *dataConnection) readDataResponses(conn net.Conn, config conf.GossipConfig, closeHandler func()) {
	// TODO: read from connection
	for {

	}

	closeHandler()
}

func (c *dataConnection) writeDataRequests(conn net.Conn, config conf.GossipConfig, closeHandler func()) {
	w := bufio.NewWriterSize(conn, config.MaxDataBodyLength()+headerSize+dataRequestMetaSize+conf.MaxTopicLength)
	// TODO: Coalesce smaller messages with channel select
	header := header{Version: 1, Op: dataOp}

	for message := range c.cli.dataMessages {
		w.Reset(w)
		streamId := <-c.streamIds
		header.StreamId = streamId
		header.BodyLength = message.BodyLength()
		message.Marshal(w, &header)
		// Create the func that will be invoked on response
		c.handlers.Store(header.StreamId, func(res dataResponse) {
			message.response <- res
			// Enqueue stream id for reuse
			c.streamIds <- streamId
		})
		if err := w.Flush(); err != nil {
			// Close connection
			break
		}
	}

	closeHandler()
}
