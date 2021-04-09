package producing

import (
	"io"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/types"
)

const maxLength = 32 * 1024

type coalescer struct {
	items    chan record
	limiter  chan bool
	config   conf.ProducerConfig
	datalog  data.Appender
	gossiper interbroker.Replicator
}

type record struct {
	topic       string
	replication types.ReplicationInfo
	length      int64
	body        io.ReadCloser
	response    chan error
}

func newCoalescer(config conf.ProducerConfig, datalog data.Appender, gossiper interbroker.Replicator) *coalescer {
	return &coalescer{
		items: make(chan record, 0),
		// Limit's to 1 outstanding group
		// The next group can be generated while the previous is being sent
		limiter:  make(chan bool, 1),
		config:   config,
		datalog:  datalog,
		gossiper: gossiper,
	}
}

func (c *coalescer) receive() {
	for {
		group := make([]record, 0)
		length := int64(0)
		// Block receiving the first item
		item, more := <-c.items

		if !more {
			// Stop receiving when channel is closed
			break
		}

		group = append(group, item)
		length += item.length

		for length < maxLength {
			// receive without blocking
			select {
			case item := <-c.items:
				group = append(group, item)
				length += item.length
			default:
				//
				break
			}
		}

		block, err := c.compress(group)

		if err != nil {
			// TODO
		}

		// Block until we can send
		c.limiter <- true

		// send in the background while the next block is generated in the foreground
		go c.send(block, group)
		// compress and crc
	}
}

func (c *coalescer) compress(group []record) (int, error) {
	// TODO: Compress and crc
	return 0, nil
}

func (c *coalescer) send(block int, group []record) {
	//TODO: Implement

	// if err := p.datalog.Append(replicationInfo.Token, topic, body); err != nil {
	// 	return err
	// }

	// if err := p.gossiper.SendToFollowers(replicationInfo, topic, body); err != nil {
	// 	return err
	// }
}

func (c *coalescer) append(topic string, replication types.ReplicationInfo, length int64, body io.ReadCloser) error {
	return nil
}

func (c *coalescer) compressAndSend() {
	defer c.done()
	// TODO: compress, crc and send
}

func (c *coalescer) done() {
	// Allow the next to be sent
	<-c.limiter
}
