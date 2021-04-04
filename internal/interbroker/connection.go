package interbroker

import (
	"net"
	"time"
)

type connectionWrapper struct {
	conn   net.Conn
	isOpen bool
}

func newOpenConnection(conn net.Conn) *connectionWrapper {
	return &connectionWrapper{conn, true}
}

func newFailedConnection() *connectionWrapper {
	return &connectionWrapper{nil, false}
}

func (c *connectionWrapper) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}

func (c *connectionWrapper) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}

func (c *connectionWrapper) Close() error {
	// Transport will invoke `Close()` when a request or ping fails
	c.isOpen = false
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
