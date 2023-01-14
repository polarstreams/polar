package producing

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/data"
	"github.com/polarstreams/polar/internal/data/topics"
	"github.com/polarstreams/polar/internal/discovery"
	"github.com/polarstreams/polar/internal/interbroker"
	"github.com/polarstreams/polar/internal/metrics"
	"github.com/polarstreams/polar/internal/producing/pooling"
	"github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
)

type Producer interface {
	types.Initializer
	types.Closer

	AcceptConnections() error
}

type coalescerGetter interface {
	Coalescer(topicName string, token types.Token, rangeIndex types.RangeIndex) *coalescer
}

func NewProducer(
	config conf.ProducerConfig,
	topicGetter topics.TopicGetter,
	leaderGetter discovery.TopologyGetter,
	datalog data.Datalog,
	gossiper interbroker.Gossiper,
) Producer {
	coalescerMap := utils.NewCopyOnWriteMap()

	return &producer{
		config:       config,
		topicGetter:  topicGetter,
		datalog:      datalog,
		gossiper:     gossiper,
		leaderGetter: leaderGetter,
		coalescerMap: coalescerMap,
		bufferPool:   pooling.NewBufferPool(config.ProducerBufferPoolSize()),
	}
}

type producer struct {
	config       conf.ProducerConfig
	topicGetter  topics.TopicGetter
	datalog      data.Datalog
	gossiper     interbroker.Gossiper
	leaderGetter discovery.TopologyGetter
	coalescerMap *utils.CopyOnWriteMap
	server       *http.Server
	bufferPool   pooling.BufferPool
}

func (p *producer) Init() error {
	// Listen to rerouted messages from other peers
	p.gossiper.RegisterReroutedMessageListener(p)
	return nil
}

func (p *producer) AcceptConnections() error {
	port := p.config.ProducerPort()
	address := utils.GetServiceAddress(port, p.leaderGetter.LocalInfo(), p.config)
	router := httprouter.New()

	router.POST(conf.TopicMessageUrl, utils.ToPostHandle(p.postMessage))
	router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		fmt.Fprintf(w, "Producer server listening on %d\n", port)
	})
	router.GET(conf.TopicMessageUrl, func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Producer server doesn't allow getting topic messages\n")
	})

	server := &http.Server{
		Addr:    address,
		Handler: router,
	}

	c := make(chan bool, 1)
	go func() {
		c <- true
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				log.Info().Msgf("Producer server stopped")
			} else {
				log.Err(err).Msgf("Producer stopped serving")
			}
		}
	}()

	<-c
	p.server = server
	log.Info().Msgf("Start listening to producers on %s", address)
	return p.acceptBinaryConnections()
}

func (p *producer) Close() {
	if err := p.server.Shutdown(context.Background()); err != nil {
		log.Err(err).Msgf("There was an error shutting down server")
	}
}

func (p *producer) OnReroutedMessage(
	topic string,
	querystring url.Values,
	contentLength int64,
	contentType string,
	body io.ReadCloser,
) error {
	return p.handleMessage(topic, querystring, contentLength, contentType, body)
}

func (p *producer) postMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	metrics.ProducerMessagesReceived.Inc()
	metrics.ProducerMessagesBodyBytes.Add(float64(r.ContentLength))

	return p.handleMessage(
		ps.ByName("topic"), r.URL.Query(), r.ContentLength, r.Header.Get(types.ContentTypeHeaderKey), r.Body)
}

// Produces or re-routes the message request
func (p *producer) handleMessage(
	topic string,
	querystring url.Values,
	contentLength int64,
	contentType string,
	body io.ReadCloser,
) error {
	if topic == "" || !p.topicGetter.Exists(topic) {
		return types.NewHttpError(http.StatusBadRequest, "Invalid topic")
	}

	if contentLength <= 0 || contentLength > int64(p.config.MaxMessageSize()) {
		log.Debug().Msgf("Invalid content length (%d) when handling message", contentLength)
		return types.NewHttpErrorf(
			http.StatusBadRequest,
			"Content length must be defined (HTTP/1.1 chunked not supported), greater than 0 and less than %d bytes",
			p.config.MaxMessageSize())
	}

	partitionKey := querystring.Get("partitionKey")
	replication := p.leaderGetter.Leader(partitionKey)
	leader := replication.Leader

	if leader == nil {
		return types.NewHttpError(
			http.StatusMisdirectedRequest,
			fmt.Sprintf("Leader for token %d could not be found", replication.Token))
	}

	if !leader.IsSelf {
		// Route the message as-is
		return p.gossiper.SendToLeader(replication, topic, querystring, contentLength, contentType, body)
	}

	// Use a buffer from the pool (may block when there isn't free space)
	buffers := p.bufferPool.Get(int(contentLength))
	defer p.bufferPool.Free(buffers)
	bodyLength, err := readBody(buffers, body)
	if err != nil {
		log.Err(err).Msgf("Producer server could not read body of expected length %d", contentLength)
		return fmt.Errorf("Producer server could not read body of expected length %d", contentLength)
	}

	timestampMicros := time.Now().UnixMicro()
	if timestamp := querystring.Get("timestamp"); timestamp != "" {
		if n, err := strconv.ParseInt(timestamp, 10, 64); err != nil {
			timestampMicros = n
		}
	}

	coalescer := p.Coalescer(topic, replication.Token, replication.RangeIndex)
	if err := coalescer.append(replication, uint32(bodyLength), timestampMicros, contentType, buffers); err != nil {
		return p.adaptCoalescerError(err)
	}
	return nil
}

func (p *producer) adaptCoalescerError(err error) error {
	inner, ok := err.(types.ProducingError)
	if ok && !inner.WasWriteAttempted() {
		return types.NewHttpError(
			http.StatusMisdirectedRequest,
			fmt.Sprintf("Producer request could not be handled at the moment: %s", err.Error()))
	}
	return err
}

func readBody(buffers [][]byte, body io.Reader) (int, error) {
	length := 0
	for i, b := range buffers {
		n, err := io.ReadFull(body, b)
		length += n
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if i == len(buffers)-1 {
					// EOF is expected on the last buffer
					return length, nil
				}
			}
			return length, err
		}
	}

	return length, nil
}

// Gets or creates the coalescer for a given topic
func (p *producer) Coalescer(topicName string, token types.Token, rangeIndex types.RangeIndex) *coalescer {
	key := coalescerKey{topicName, token, rangeIndex}
	c, loaded, _ := p.coalescerMap.LoadOrStore(key, func() (interface{}, error) {
		return newCoalescer(topicName, token, rangeIndex, p.leaderGetter, p.gossiper, p.config), nil
	})

	if !loaded {
		log.Debug().Msgf("Created coalescer for topic '%s' (%d/%d)", topicName, token, rangeIndex)
	}

	return c.(*coalescer)
}

type coalescerKey struct {
	topicName  string
	token      types.Token
	rangeIndex types.RangeIndex
}
