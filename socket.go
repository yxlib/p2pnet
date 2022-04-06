// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"errors"
	"net"
	"time"

	"github.com/yxlib/yx"
)

var (
	ErrSockServIpConnLimit = errors.New("ip connect reach limit")
)

//========================
//       SockServ
//========================
type SockServ struct {
	*BaseServer
	l net.Listener
}

func NewSockServ(peerMgr PeerMgr, ipConnCntLimit uint32, headerFactory PackHeaderFactory, maxReadQue uint32, maxWriteQue uint32) *SockServ {
	return &SockServ{
		BaseServer: NewBaseServ(peerMgr, ipConnCntLimit, headerFactory, maxReadQue, maxWriteQue),
		l:          nil,
	}
}

func (s *SockServ) Listen(network string, address string) error {
	l, err := net.Listen(network, address)
	if err != nil {
		return s.ec.Throw("Listen", err)
	}

	s.l = l

	for {
		// accept
		c, err := s.accept()
		if err != nil {
			break
		}

		// ip limit
		if s.HandleIpLimit(c) {
			continue
		}

		s.OpenConn(c)
	}

	return nil
}

func (s *SockServ) Close() error {
	return s.l.Close()
	// err := s.l.Close()
	// if err != nil {
	// 	return err
	// }

	// s.Stop()
	// return nil
}

func (s *SockServ) accept() (net.Conn, error) {
	c, err := s.l.Accept()
	if err != nil {
		return nil, s.ec.Throw("Accept", err)
	}

	return c, nil
}

//========================
//       SockClient
//========================
type SockClient struct {
	ec *yx.ErrCatcher
}

func NewSockClient() *SockClient {
	return &SockClient{
		ec: yx.NewErrCatcher("p2pnet.SockClient"),
	}
}

func (c *SockClient) DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, c.ec.Throw("DialTimeout", err)
	}

	return conn, nil
}
