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
	config conf.Config,
	topicGetter topics.TopicGetter,
	leaderGetter discovery.LeaderGetter,
	datalog data.Datalog,
	gossiper interbroker.Gossiper,
) Producer {
	return &producer{
		config,
		topicGetter,
		leaderGetter,
		datalog,
		gossiper,
	}
}

type producer struct {
	config       conf.Config
	topicGetter  topics.TopicGetter
	leaderGetter discovery.LeaderGetter
	datalog      data.Datalog
	gossiper     interbroker.Gossiper
}

func (p *producer) Init() error {
	return nil
}

func (p *producer) AcceptConnections() error {
	port := p.config.ProducerPort()
	address := fmt.Sprintf(":%d", port)
	router := httprouter.New()

	router.POST(conf.TopicMessageUrl, ToHandle(p.postMessage))
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

type HandleWithError func(http.ResponseWriter, *http.Request, httprouter.Params) error

func ToHandle(he HandleWithError) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := he(w, r, ps); err != nil {
			httpErr, ok := err.(types.HttpError)

			if !ok {
				log.Err(err).Msg("Unexpected error when producing")
				http.Error(w, "Internal server error", 500)
				return
			}

			w.WriteHeader(httpErr.StatusCode())
			// The message is supposed to be user friendly
			fmt.Fprintf(w, err.Error())
		}
	}
}

func (p *producer) postMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	var topic = strings.TrimSpace(ps.ByName("topic"))
	if topic == "" || !p.topicGetter.Exists(topic) {
		return types.NewHttpError(http.StatusBadRequest, "Invalid topic")
	}

	partitionKey := r.URL.Query().Get("partitionKey")
	replicationInfo := p.leaderGetter.GetLeader(partitionKey)
	leader := replicationInfo.Leader

	if leader == nil {
		return fmt.Errorf("Leader was not found")
	}

	// TODO: Global backpressure based on ContentLength
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil
	}

	if leader.IsSelf {
		p.datalog.Append(replicationInfo.Token, topic, body)
		// gossiper.SendToFollower(token, topic, body)
	} else {
		// gossiper.SendToLeader(token, topic, body)
	}

	fmt.Fprintf(w, "OK")
	return nil
}
