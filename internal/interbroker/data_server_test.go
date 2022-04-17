package interbroker

import (
	"time"

	"github.com/barcostreams/barco/internal/test/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
)

var _ = Describe("peerDataServer", func() {
	Describe("writeResponses()", func() {
		It("should coalesce responses", func() {
			conn := &fakes.Connection{
				WriteBuffers: make([][]byte, 0),
			}
			server := &peerDataServer{
				conn:        conn,
				initialized: true,
				responses:   make(chan dataResponse, 100),
			}

			startWriting := make(chan bool)
			go func() {
				for i := 0; i < 10; i++ {
					message := &emptyResponse{
						streamId: streamId(i),
						op:       chunkReplicationResponseOp}
					server.responses <- message
				}
				log.Debug().Msgf("Closing response channel")
				startWriting <- true
				time.Sleep(200 * time.Millisecond)

				server.responses <- newErrorResponse("Hello", &header{StreamId: 10})
				close(server.responses)
			}()

			<-startWriting
			server.writeResponses()

			Expect(conn.WriteBuffers).To(HaveLen(2))
			Expect(conn.WriteBuffers[0]).To(HaveLen(headerSize*10), "The first buffer with the empty messages")
			Expect(conn.WriteBuffers[1]).To(HaveLen(headerSize+len("Hello")), "The second buffer with the error messages")
		})

		It("should send buffered responses", func() {
			conn := &fakes.Connection{
				WriteBuffers: make([][]byte, 0),
			}
			server := &peerDataServer{
				conn:        conn,
				initialized: true,
				responses:   make(chan dataResponse, 100),
			}

			startWriting := make(chan bool)
			bufferedResponse := &fileStreamResponse{
				streamId: 1,
				op:       fileStreamResponseOp,
				buf:      []byte{0, 1, 2, 3},
			}

			go func() {
				server.responses <- &emptyResponse{
					streamId: 0,
					op:       chunkReplicationResponseOp,
				}
				server.responses <- bufferedResponse
				server.responses <- &emptyResponse{
					streamId: 2,
					op:       chunkReplicationResponseOp,
				}
				startWriting <- true
				close(server.responses)
			}()

			<-startWriting
			server.writeResponses()

			Expect(conn.WriteBuffers).To(HaveLen(3))
			Expect(conn.WriteBuffers[0]).To(HaveLen(headerSize*2), "The first buffer with the empty messages")
			Expect(conn.WriteBuffers[1]).To(HaveLen(headerSize), "The 2nd buffer with the buffered responses")
			Expect(conn.WriteBuffers[2]).To(Equal(bufferedResponse.buf), "The 3rd buffer with the body")
		})
	})
})
