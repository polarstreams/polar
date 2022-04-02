package fakes

import (
	"net"
	"time"
)

type Connection struct {
	WriteBuffers [][]byte
}

func (c *Connection) Read(b []byte) (n int, err error) {
	return len(b), nil
}

func (c *Connection) Write(b []byte) (n int, err error) {
	c.WriteBuffers = append(c.WriteBuffers, b)
	return len(b), nil
}

func (c *Connection) Close() error {
	return nil
}

func (c *Connection) LocalAddr() net.Addr {
	return nil
}

func (c *Connection) RemoteAddr() net.Addr {
	return nil
}

func (c *Connection) SetDeadline(t time.Time) error {
	return nil
}

func (c *Connection) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *Connection) SetWriteDeadline(t time.Time) error {
	return nil
}
