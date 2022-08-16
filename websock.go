// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yxlib/yx"
)

//========================
//      WebSockConn
//========================
type WebSockConn struct {
	conn   *websocket.Conn
	buff   []byte
	offset uint32
}

func NewWebSockConn(conn *websocket.Conn) *WebSockConn {
	return &WebSockConn{
		conn:   conn,
		buff:   nil,
		offset: 0,
	}
}

func (c *WebSockConn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	if len(c.buff) > 0 {
		return c.readFromBuff(b)
	}

	_, p, err := c.conn.ReadMessage()
	if err != nil {
		return 0, err
	}

	if len(p) == 0 {
		return 0, nil
	}

	c.buff = p
	c.offset = 0
	return c.readFromBuff(b)
}

func (c *WebSockConn) Write(b []byte) (n int, err error) {
	writeLen := len(b)
	err = c.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}

	return writeLen, nil
}

func (c *WebSockConn) Close() error {
	return c.conn.Close()
}

func (c *WebSockConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *WebSockConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *WebSockConn) SetDeadline(t time.Time) error {
	err := c.conn.SetReadDeadline(t)
	if err != nil {
		return err
	}

	err = c.conn.SetWriteDeadline(t)
	return err
}

func (c *WebSockConn) SetReadDeadline(t time.Time) error {
	// return c.conn.SetReadDeadline(t)
	return nil
}

func (c *WebSockConn) SetWriteDeadline(t time.Time) error {
	// return c.conn.SetWriteDeadline(t)
	return nil
}

func (c *WebSockConn) readFromBuff(b []byte) (n int, err error) {
	needLen := len(b)
	buffLen := len(c.buff) - int(c.offset)
	copyLen := buffLen
	if copyLen > needLen {
		copyLen = needLen
	}

	copy(b, c.buff[c.offset:c.offset+uint32(copyLen)])
	c.offset += uint32(copyLen)
	if c.offset == uint32(buffLen) {
		c.buff = nil
		c.offset = 0
	}

	return copyLen, nil
}

//========================
//       WebSockServ
//========================
type WebSockServ struct {
	*BaseServer
	upgrader *websocket.Upgrader

	httpSrv     *http.Server
	evtShutdown *yx.Event
}

func NewWebSockServ(peerMgr PeerMgr, ipConnCntLimit uint32, headerFactory PackHeaderFactory, maxReadQue uint32, maxWriteQue uint32) *WebSockServ {
	return &WebSockServ{
		BaseServer: NewBaseServ(peerMgr, ipConnCntLimit, headerFactory, maxReadQue, maxWriteQue),
		upgrader:   nil,

		httpSrv:     nil,
		evtShutdown: yx.NewEvent(),
	}
}

var DefaultWsUpgrader = &websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (s *WebSockServ) Init(pattern string, upgrader *websocket.Upgrader) {
	// http.HandleFunc(pattern, s.serveHttp)

	s.upgrader = upgrader
	if s.upgrader == nil {
		s.upgrader = DefaultWsUpgrader
	}
}

func (s *WebSockServ) Listen(network string, address string) error {
	// err := http.ListenAndServe(address, nil)
	// if err != nil {
	// 	return s.ec.Throw("Listen", err)
	// }

	// return nil

	s.httpSrv = &http.Server{
		Addr:    address,
		Handler: s,
	}

	err := s.httpSrv.ListenAndServe()
	if err != nil {
		if err != http.ErrServerClosed {
			return err
		}
	}

	s.evtShutdown.Wait()
	return nil
}

func (s *WebSockServ) Close() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := s.httpSrv.Shutdown(ctx)
	if err != nil {
		return err
	}

	s.evtShutdown.Send()
	return nil
}

func (s *WebSockServ) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.ec.Catch("serveHttp", &err)
		return
	}

	c := NewWebSockConn(ws)
	if s.HandleIpLimit(c) {
		return
	}

	s.OpenConn(c)
}

//========================
//       WebSockClient
//========================
type WebSockClient struct {
	ec *yx.ErrCatcher
}

func NewWebSockClient() *WebSockClient {
	return &WebSockClient{
		ec: yx.NewErrCatcher("p2pnet.WebSockClient"),
	}
}

func (c *WebSockClient) DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error) {
	ws, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		return nil, c.ec.Throw("DialTimeout", err)
	}

	return NewWebSockConn(ws), nil
}
