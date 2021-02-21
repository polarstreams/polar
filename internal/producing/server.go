package producing

import (
	"fmt"
	"net/http"

	"github.com/jorgebay/soda/internal/configuration"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Producer interface {
	Init() error

	AcceptConnections() error
}

func NewProducer(config configuration.Config) Producer {
	return &producer{
		config,
	}
}

type producer struct {
	config configuration.Config
}

func (p *producer) Init() error {
	return nil
}

func (p *producer) AcceptConnections() error {
	port := p.config.ProducerPort()
	address := fmt.Sprintf(":%d", port)
	router := httprouter.New()
    router.GET("/", Index)

	h2s := &http2.Server{}
	server := &http.Server{
		Addr: address,
		Handler: h2c.NewHandler(router, h2s),
	}

	http2.ConfigureServer(server, h2s)

	c := make(chan bool, 1)
	go func () {
		log.Info().Int32("port", port).Msg("Start listening to producers")
		c <- true
		if err := server.ListenAndServe(); err != nil {
			log.Fatal().Err(err)
		}
	}()

	<-c
	return nil
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
    fmt.Fprint(w, "Hello from soda producer\n")
}