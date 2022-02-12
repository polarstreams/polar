package interbroker

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog/log"
)

const maxStreamIds = 1024

// dataConnection represents a client TCP data connection
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

	log.Debug().Msgf("Sending startup data message to %s", conn.RemoteAddr())
	if err = sendStartupMessage(conn); err != nil {
		conn.Close()
		log.Warn().Msgf("Startup message could not be sent to %s: %s", conn.RemoteAddr(), err.Error())
		return nil, fmt.Errorf("Startup message could not be sent: %s", err.Error())
	}

	log.Debug().Msgf("Startup sent, data connection to peer %s is ready", conn.RemoteAddr())

	var once sync.Once
	closed := make(chan bool)
	closeHandler := func(userType string) {
		once.Do(func() {
			log.Info().Msgf("Peer data client %s closing connection to %s", userType, conn.RemoteAddr())
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

func sendStartupMessage(conn net.Conn) error {
	buffer := &bytes.Buffer{}
	header := &header{
		Version:    1,
		StreamId:   0,
		Op:         startupOp,
		BodyLength: 0,
	}
	if err := writeHeader(buffer, header); err != nil {
		return err
	}

	if n, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	} else if n < buffer.Len() {
		return fmt.Errorf("Write too short")
	}

	responseHeaderBuffer := make([]byte, headerSize)
	if _, err := io.ReadFull(conn, responseHeaderBuffer); err != nil {
		return err
	}
	responseHeader, err := readHeader(responseHeaderBuffer)

	if err != nil {
		return err
	}

	if responseHeader.Op != readyOp {
		return fmt.Errorf("Expected ready message, obtained op %d", responseHeader.Op)
	}

	return nil
}

func (c *dataConnection) readDataResponses(conn net.Conn, config conf.GossipConfig, closeHandler func(string)) {
	r := bufio.NewReaderSize(conn, receiveBufferSize)
	headerBuffer := make([]byte, headerSize)
	bodyBuffer := make([]byte, maxDataResponseSize)
	for {
		if n, err := io.ReadFull(r, headerBuffer); err != nil {
			log.Warn().Err(err).Int("n", n).Msg("There was an error reading header from peer server")
			break
		}
		header, err := readHeader(headerBuffer)
		if err != nil {
			log.Warn().Msg("Invalid data header from peer, closing connection")
			break
		}

		body := bodyBuffer[:header.BodyLength]
		if _, err := io.ReadFull(r, body); err != nil {
			log.Warn().Msg("There was an error reading body from peer")
			break
		}

		response := unmarshallResponse(header, body)
		value, ok := c.handlers.LoadAndDelete(header.StreamId)

		if !ok {
			log.Error().Uint16("streamId", uint16(header.StreamId)).Msg("Invalid message from the server")
			break
		}
		handler := value.(func(dataResponse))
		handler(response)
	}

	closeHandler("reader")
}

func (c *dataConnection) writeDataRequests(conn net.Conn, config conf.GossipConfig, closeHandler func(string)) {
	w := utils.NewBufferCap(config.MaxDataBodyLength() + headerSize + dataRequestMetaSize + conf.MaxTopicLength)
	header := header{Version: 1, Op: dataReplicationOp}

	for message := range c.cli.dataMessages {
		w.Reset()
		streamId := <-c.streamIds
		header.StreamId = streamId
		header.BodyLength = message.BodyLength()
		// Each message represents a group of messages
		message.Marshal(w, &header)

		// After copying to the underlying buffer, we check whether we can use it
		if message.ctxt.Err() != nil {
			// I can't use the message as it reached the deadline/was cancelled
			// The message slice is out of date
			c.streamIds <- streamId
			//TODO: Use a better error
			//TODO: Add metrics by data connection host: replicatedMissedWrites
			message.response <- newErrorResponse(message.ctxt.Err().Error(), &header)
			continue
		}

		// Create the func that will be invoked on response
		c.handlers.Store(header.StreamId, func(res dataResponse) {
			message.response <- res
			// Enqueue stream id for reuse
			c.streamIds <- streamId
		})

		if n, err := conn.Write(w.Bytes()); err != nil {
			log.Warn().Err(err).Msg("Peer data client flush resulted in error")
			break
		} else if n < w.Len() {
			log.Warn().Msg("Peer data client write was not able to send all the data")
			break
		}
	}

	closeHandler("writer")
}
