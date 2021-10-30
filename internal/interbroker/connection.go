package interbroker

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// connectionWrapper represents a http client connection
type connectionWrapper struct {
	conn             net.Conn
	isOpen           atomic.Value
	closeHandlerOnce sync.Once
	closeHandler     func()
}

func newOpenConnection(conn net.Conn, closeHandler func()) *connectionWrapper {
	isOpen := atomic.Value{}
	isOpen.Store(true)
	return &connectionWrapper{
		conn:         conn,
		isOpen:       isOpen,
		closeHandler: closeHandler,
	}
}

func newFailedConnection() *connectionWrapper {
	isOpen := atomic.Value{}
	isOpen.Store(false)
	return &connectionWrapper{
		conn:         nil,
		isOpen:       isOpen,
		closeHandler: nil,
	}
}

// IsOpen() returns true when the connection is known to be open.
func (c *connectionWrapper) IsOpen() bool {
	v := c.isOpen.Load()
	if v == nil {
		return false
	}
	return v.(bool)
}

func (c *connectionWrapper) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}

func (c *connectionWrapper) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}

func (c *connectionWrapper) Close() error {
	// Transport will invoke `Close()` when a request or ping fails
	c.isOpen.Store(false)
	if c.closeHandler != nil {
		go c.closeHandlerOnce.Do(c.closeHandler)
	}
	return c.conn.Close()
}

func (c *connectionWrapper) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *connectionWrapper) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connectionWrapper) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connectionWrapper) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connectionWrapper) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
