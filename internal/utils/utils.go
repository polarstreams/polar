package utils

import (
	"fmt"
	"net/http"

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
