package utils

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

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
			adaptHttpErr(err, w)
		}
	}
}

func adaptHttpErr(err error, w http.ResponseWriter) {
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

// ToPostHandle wraps a handle func with error, returns plain text "OK" and converts it to a `httprouter.Handle`
func ToPostHandle(he HandleWithError) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := he(w, r, ps); err != nil {
			adaptHttpErr(err, w)
		} else {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("OK"))
		}
	}
}

// NewBufferCap returns a buffer with the initial provided initial capacity
func NewBufferCap(initialCap int) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, initialCap))
}

// GetServiceAddress determines whether it should be bind to all interfaces or it should use a single host name
func GetServiceAddress(port int, discoverer discovery.TopologyGetter, config conf.BasicConfig) string {
	address := fmt.Sprintf(":%d", port)

	if !config.ListenOnAllAddresses() {
		info := discoverer.LocalInfo()
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

func ToUnixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func FromUnixMillis(millis int64) time.Time {
	return time.Unix(0, millis*int64(time.Millisecond))
}

func NaturalRingOrder(size int) []uint32 {
	if size == 3 {
		return []uint32{0, 1, 2}
	}
	ringSize6 := []uint32{0, 3, 1, 4, 2, 5}
	if size == 6 {
		return ringSize6
	}

	// Rings are 3*2^n (6, 12, 24, ...)
	exponent := math.Log2(float64(size / 3))
	lastExponent := exponent - 1
	lastPow2 := math.Pow(2, lastExponent)

	// To calculate the odd numbers
	lastStartIndex := 3 * math.Pow(2, lastExponent)

	result := make([]uint32, size)

	for i := 0; i < size; i++ {
		if i%2 == 1 {
			result[i] = uint32(lastStartIndex + math.Floor(float64(i)/2))
			continue
		}

		if i%int(lastPow2) == 0 {
			// The position of ring size 6
			result[i] = ringSize6[int(math.Floor(float64(i)/lastPow2))]
			continue
		}

		for j := 1.0; j <= exponent-2; j++ {
			e := exponent - j
			step := int(math.Pow(2, e))
			if i%step == int(math.Pow(2, e-1)) {
				startSeries := 3 * math.Pow(2, j)
				result[i] = uint32(startSeries) + uint32(i/step)
			}
		}
	}

	return result
}
