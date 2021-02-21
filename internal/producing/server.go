package producing

import (
	"fmt"
	"net/http"

	"github.com/jorgebay/soda/internal/configuration"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
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
	router := httprouter.New()
    router.GET("/", Index)

	c := make(chan bool, 1)
	go func () {
		port := p.config.ProducerPort()
		address := fmt.Sprintf(":%d", port)
		log.Info().Int32("port", port).Msg("Start listening to producers")
		c <- true
		if err := http.ListenAndServe(address, router); err != nil {
			log.Fatal().Err(err)
		}
	}()

	<-c
	return nil
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
    fmt.Fprint(w, "Welcome!\n")
}