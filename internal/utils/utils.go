package utils

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/types"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
)

type HandleWithError func(http.ResponseWriter, *http.Request, httprouter.Params) error

// ToHandle wraps a handle func with error and converts it to a `httprouter.Handle`
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

// NewBufferCap returns a buffer with the initial provided initial capacity
func NewBufferCap(initialCap int) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, initialCap))
}

// GetServiceAddress determines whether it should be bind to all interfaces or it should use a single host name
func GetServiceAddress(port int, discoverer discovery.LeaderGetter, config conf.BasicConfig) string {
	address := fmt.Sprintf(":%d", port)

	if !config.ListenOnAllAddresses() {
		info := discoverer.GetBrokerInfo()
		// Use the provided name / address
		address = fmt.Sprintf("%s:%d", info.HostName, port)
	}

	return address
}

func ToCsv(values []int) string {
	result := ""
	for _, v := range values {
		if len(result) > 0 {
			result += ","
		}
		result += strconv.Itoa(v)
	}

	return result
}
