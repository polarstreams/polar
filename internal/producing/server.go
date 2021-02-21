package producing

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Producer interface {
	Init() error

	AcceptConnections() error
}

func NewProducer(config conf.Config, topicGetter data.TopicGetter) Producer {
	return &producer{
		config,
		topicGetter,
	}
}

type producer struct {
	config      conf.Config
	topicGetter data.TopicGetter
}

func (p *producer) Init() error {
	return nil
}

func (p *producer) AcceptConnections() error {
	port := p.config.ProducerPort()
	address := fmt.Sprintf(":%d", port)
	router := httprouter.New()

	router.POST(conf.TopicMessageUrl, p.postMessage)
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

func (p *producer) postMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var topic = strings.TrimSpace(ps.ByName("topic"))
	if topic == "" {
		badRequestResponse(w, "Invalid topic")
		return
	}

	//TODO: Lookup for topic

	var partitionKey = r.URL.Query().Get("partitionKey")
	fmt.Fprintf(w, "Received post %s %s\n", ps.ByName("topic"), partitionKey)

	fmt.Fprintf(w, "OK")
}

func badRequestResponse(w http.ResponseWriter, message string) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, message)
}
