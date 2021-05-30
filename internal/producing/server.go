package producing

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/data/topics"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Producer interface {
	types.Initializer

	AcceptConnections() error
}

func NewProducer(
	config conf.ProducerConfig,
	topicGetter topics.TopicGetter,
	leaderGetter discovery.LeaderGetter,
	datalog data.Datalog,
	gossiper interbroker.Gossiper,
) Producer {
	coalescerMap := utils.NewCopyOnWriteMap()

	return &producer{
		config,
		topicGetter,
		datalog,
		gossiper,
		leaderGetter,
		coalescerMap,
	}
}

type producer struct {
	config       conf.ProducerConfig
	topicGetter  topics.TopicGetter
	datalog      data.Datalog
	gossiper     interbroker.Gossiper
	leaderGetter discovery.LeaderGetter
	// We use a single coalescer per topics
	coalescerMap *utils.CopyOnWriteMap
}

func (p *producer) Init() error {
	return nil
}

func (p *producer) AcceptConnections() error {
	port := p.config.ProducerPort()
	address := fmt.Sprintf(":%d", port)
	router := httprouter.New()

	router.POST(conf.TopicMessageUrl, utils.ToHandle(p.postMessage))
	router.GET("/status", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		fmt.Fprintf(w, "Producer server listening on %d\n", port)
	})
	router.GET(conf.TopicMessageUrl, func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Producer server doesn't allow getting topic messages\n")
	})

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    address,
		Handler: h2c.NewHandler(router, h2s),
	}

	http2.ConfigureServer(server, h2s)

	c := make(chan bool, 1)
	go func() {
		c <- true
		if err := server.ListenAndServe(); err != nil {
			log.Fatal().Err(err)
		}
	}()

	<-c
	log.Info().Msgf("Start listening to producers on port %d", port)
	return nil
}

func (p *producer) postMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	var topic = strings.TrimSpace(ps.ByName("topic"))
	if topic == "" || !p.topicGetter.Exists(topic) {
		return types.NewHttpError(http.StatusBadRequest, "Invalid topic")
	}

	if r.ContentLength <= 0 || r.ContentLength > int64(p.config.MaxMessageSize()) {
		return types.NewHttpErrorf(
			http.StatusBadRequest,
			"Content length must be defined (HTTP/1.1 chunked not supported), greater than 0 and less than %d bytes",
			p.config.MaxMessageSize())
	}

	partitionKey := r.URL.Query().Get("partitionKey")
	replicationInfo := p.leaderGetter.GetLeader(partitionKey)
	leader := replicationInfo.Leader

	if leader == nil {
		return fmt.Errorf("Leader was not found")
	}

	p.config.FlowController().Allocate(int(r.ContentLength))
	defer p.config.FlowController().Free(int(r.ContentLength))

	if !leader.IsSelf {
		// TODO: Define whether ReadAll is needed
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return err
		}

		// Route the message as is
		if err := p.gossiper.SendToLeader(replicationInfo, topic, body); err != nil {
			return err
		}
	}

	coalescer, err := p.getCoalescer(topic, replicationInfo.Token)
	if err != nil {
		return err
	}
	if err := coalescer.append(replicationInfo, r.ContentLength, r.Body); err != nil {
		return err
	}

	fmt.Fprintf(w, "OK")
	return nil
}

func (p *producer) getCoalescer(topicName string, token types.Token) (*coalescer, error) {
	topic := types.TopicDataId{
		Name:  topicName,
		Token: token,
		// TODO: Define genId (i.e: v1)
		GenId: 0,
	}
	c, loaded, err := p.coalescerMap.LoadOrStore(topicName, func() (interface{}, error) {
		// Creating the appender is a blocking call
		return newCoalescer(topic, p.config, p.gossiper)
	})

	if err != nil {
		return nil, err
	}

	if !loaded {
		log.Debug().Msgf("Created coalescer for topic '%s'", topicName)
	}

	return c.(*coalescer), nil
}
