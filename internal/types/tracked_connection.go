package types

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// TrackedConnection represents a net connection over http
// that contains information about whether it's open/closed.
//
// It benefits from the fact that a Transport will invoke `Close()`
// when a http request or http2 ping fails
type TrackedConnection struct {
	conn             net.Conn
	isOpen           atomic.Value
	closeHandlerOnce sync.Once
	closeHandler     func(*TrackedConnection)
	id               uuid.UUID
	lastRead         int64 // TODO: Remove
}

// Creates a new TrackedConnection using the provided tcp conn.
// It invokes the close handler once it's closed.
func NewTrackedConnection(conn net.Conn, closeHandler func(*TrackedConnection)) *TrackedConnection {
	isOpen := atomic.Value{}
	isOpen.Store(true)
	return &TrackedConnection{
		conn:         conn,
		isOpen:       isOpen,
		closeHandler: closeHandler,
		id:           uuid.New(),
		lastRead:     time.Now().UnixMilli(),
	}
}

// Creates a new TrackedConnection using the provided tcp conn.
func NewFailedConnection() *TrackedConnection {
	isOpen := atomic.Value{}
	isOpen.Store(false)
	return &TrackedConnection{
		conn:         nil,
		isOpen:       isOpen,
		closeHandler: nil,
		id:           uuid.New(),
	}
}

// IsOpen() returns true when the connection is known to be open.
func (c *TrackedConnection) IsOpen() bool {
	v := c.isOpen.Load()
	if v == nil {
		return false
	}
	return v.(bool)
}

func (c *TrackedConnection) Id() uuid.UUID {
	return c.id
}

// Sets the last tracked read for deadlines
func (c *TrackedConnection) SetAsRead() {
	atomic.StoreInt64(&c.lastRead, time.Now().UnixMilli())
}

// Gets the last tracked read for read timeouts
func (c *TrackedConnection) LastRead() time.Time {
	return time.UnixMilli(atomic.LoadInt64(&c.lastRead))
}

func (c *TrackedConnection) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}

func (c *TrackedConnection) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}

func (c *TrackedConnection) Close() error {
	// Transport will invoke `Close()` when a request or ping fails
	c.isOpen.Store(false)
	if c.closeHandler != nil {
		go c.closeHandlerOnce.Do(func() {
			c.closeHandler(c)
		})
	}
	return c.conn.Close()
}

func (c *TrackedConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *TrackedConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *TrackedConnection) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *TrackedConnection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *TrackedConnection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
