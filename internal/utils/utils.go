package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/types"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
)

type HandleWithError func(http.ResponseWriter, *http.Request, httprouter.Params) error

var jitterRng = rand.New(rand.NewSource(time.Now().UnixNano()))

// ToHandle wraps a handle func with error and converts it to a `httprouter.Handle`
func ToHandle(he HandleWithError) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := he(w, r, ps); err != nil {
			adaptHttpErr(err, w)
		}
	}
}

// MaxVersion gets the maximum version value of the non-nil generations provided.
// Defaults to zero.
func MaxVersion(values ...*types.Generation) types.GenVersion {
	result := types.GenVersion(0)
	for _, v := range values {
		if v != nil && v.Version > result {
			result = v.Version
		}
	}

	return result
}

func adaptHttpErr(err error, w http.ResponseWriter) {
	httpErr, ok := err.(types.HttpError)

	if !ok {
		log.Err(err).Msg("Unexpected error when serving")
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
func GetServiceAddress(port int, localInfo *types.BrokerInfo, config conf.BasicConfig) string {
	address := fmt.Sprintf(":%d", port)

	if !config.ListenOnAllAddresses() {
		// Use the provided name / address
		address = fmt.Sprintf("%s:%d", localInfo.HostName, port)
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

// For fatal errors, it logs and exists
func PanicIfErr(err error, message string) {
	if err != nil {
		log.Panic().Err(err).Msg(message)
	}
}

func CreateErrAndLog(format string, v ...interface{}) error {
	message := fmt.Sprintf(format, v...)
	log.Error().Msgf(message)
	return errors.New(message)
}

func ToBlob(v uuid.UUID) []byte {
	// Can't error
	blob, _ := v.MarshalBinary()
	return blob
}

// BinarySize gets the amount of bytes required to write the value,
// validating that all types are fixed-sized.
func BinarySize(v interface{}) int {
	size := binary.Size(v)
	if size <= 0 {
		panic(fmt.Sprintf("Size of type %v could not be determined", v))
	}
	return size
}

// Adds a +-5% jitter to the duration with millisecond resolution
func Jitter(t time.Duration) time.Duration {
	rand.Float64()
	ms := float64(t.Milliseconds())
	maxJitter := 0.1 * ms
	if maxJitter < 1 {
		panic("Delay should be at least 20ms")
	}
	jitterRange := jitterRng.Float64() * maxJitter
	startJitter := 0.05 * ms
	return time.Duration(ms-startJitter+jitterRange) * time.Millisecond
}

// Sets the response status as 204 (NoContent) w/ no cache and optionally setting the retry after header.
func NoContentResponse(w http.ResponseWriter, retryAfter int) {
	w.WriteHeader(http.StatusNoContent)
	w.Header().Set("Cache-Control", "no-store")
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
}

func ContainsString(values []string, key string) bool {
	for _, v := range values {
		if v == key {
			return true
		}
	}
	return false
}

func ContainsToken(values []types.TokenRanges, key types.Token) bool {
	for _, v := range values {
		if v.Token == key {
			return true
		}
	}
	return false
}

// Reads the body as a string and closes it
func ReadBodyClose(resp *http.Response) (string, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	return string(body), nil
}

// For a given number, it returns the last ring length that can contain it.
// For example: given 3 it returns 3; for 4 -> 3; for 5 -> 3; for 7 -> 6
func ValidRingLength(length int) int {
	if length < 3 {
		return 3
	}
	// Rings are 3 * 2^n
	exponent := math.Floor(math.Log2(float64(length) / 3))
	return int(3 * math.Exp2(exponent))
}
