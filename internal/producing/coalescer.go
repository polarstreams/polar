package producing

import (
	"io"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

type coalescer struct {
	items    chan *record
	limiter  chan bool
	topic    string
	config   conf.ProducerConfig
	appender data.Appender
	gossiper interbroker.Replicator
	offset   uint64
}

type record struct {
	replication types.ReplicationInfo
	length      int64
	body        io.ReadCloser
	offset      uint64
	response    chan error
}

func newCoalescer(topic string, config conf.ProducerConfig, appender data.Appender, gossiper interbroker.Replicator) *coalescer {
	c := &coalescer{
		items: make(chan *record, 0),
		// Limit's to 1 outstanding group
		// The next group can be generated while the previous is being sent
		limiter:  make(chan bool, 1),
		topic:    topic,
		config:   config,
		appender: appender,
		gossiper: gossiper,
		offset:   0, // TODO: Set the initial offset
	}
	// Start receiving in the background
	go c.receive()
	return c
}

func (c *coalescer) add(group []record, item *record, length *int64) ([]record, *record) {
	if *length+item.length > int64(c.config.MaxGroupSize()) {
		// Return a non-nil record as a signal that it was not appended
		return group, item
	}
	*length += item.length
	item.offset = c.offset
	c.offset++
	group = append(group, *item)
	return group, nil
}

func (c *coalescer) receive() {
	var item *record = nil
	for {
		group := make([]record, 0)
		length := int64(0)

		// Block receiving the first item when there isn't a buffered item
		if item == nil {
			item = <-c.items
		}

		// Either there was a buffered item or we just received it
		group, _ = c.add(group, item, &length)

		canAddNext := true
		for canAddNext {
			// Receive without blocking until there are no more items
			// or the max length for a group was reached
			select {
			case item = <-c.items:
				group, item = c.add(group, item, &length)
				if item != nil {
					// The group can't contain the new item
					canAddNext = false
				}
			default:
				canAddNext = false
			}
		}

		data, err := c.compress(group)

		if err != nil {
			log.Err(err).Msg("Unexpected compression error")
			for _, r := range group {
				r.response <- err
			}
			// The group will not be persisted, reset the offset
			c.offset = group[0].offset
			continue
		}

		// TODO: Define whether the previous segment must be closed and pass it to the replicas
		_, err = c.appender.Append(data)

		if err != nil {
			// TODO: Define what to do
		}

		// Block until we can send
		c.limiter <- true

		// send in the background while the next block is generated in the foreground
		go c.send(data, group)
	}
}

func (c *coalescer) compress(group []record) ([]byte, error) {
	// TODO: Compress and crc
	return nil, nil
}

func (c *coalescer) send(block []byte, group []record) {
	defer c.doneSending()
	//TODO: Implement

	// if err := p.gossiper.SendToFollowers(replicationInfo, topic, body); err != nil {
	// 	return err
	// }
}

func (c *coalescer) append(replication types.ReplicationInfo, length int64, body io.ReadCloser) error {
	record := &record{
		replication: replication,
		length:      length,
		body:        body,
		response:    make(chan error, 1),
	}
	c.items <- record
	return <-record.response
}

func (c *coalescer) doneSending() {
	// Allow the next to be sent
	<-c.limiter
}
