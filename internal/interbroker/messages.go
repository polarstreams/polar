package interbroker

type opcode uint8

const (
	startupOp opcode = iota
	readyOp
	errorOp
	dataOp
	dataResponseOp
)

// header is the interbroker message header
type header struct {
	Version    uint8
	StreamId   uint16
	Op         opcode
	BodyLength uint32
	// TODO: Add CRC to header
}

const headerSize = 1 + // version
	2 + // stream id
	1 + // op
	4 // length
