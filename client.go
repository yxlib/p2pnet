// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"net"
	"time"
)

type Client interface {
	DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error)
}

type SimpleClient struct {
	cli           Client
	peerMgr       PeerMgr
	headerFactory PackHeaderFactory
	packPool      *PackPool
	maxReadQue    uint32
	maxWriteQue   uint32
}

func NewSimpleClient(cli Client, peerMgr PeerMgr, headerFactory PackHeaderFactory, maxReadQue uint32, maxWriteQue uint32) *SimpleClient {
	return &SimpleClient{
		cli:           cli,
		peerMgr:       peerMgr,
		headerFactory: headerFactory,
		packPool:      NewPackPool(headerFactory),
		maxReadQue:    maxReadQue,
		maxWriteQue:   maxWriteQue,
	}
}

func (c *SimpleClient) DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error) {
	conn, err := c.cli.DialTimeout(network, address, timeout)
	return conn, err
}

func (c *SimpleClient) OpenConn(peerType uint32, peerNo uint32, network string, address string, timeout time.Duration, bRegister bool) error {
	conn, err := c.cli.DialTimeout(network, address, timeout)
	if err != nil {
		return err
	}

	p := NewPeer(peerType, peerNo, conn, c.maxReadQue, c.maxWriteQue)
	p.SetPackPool(c.packPool)
	c.peerMgr.AddPeer(p, false, bRegister)
	return nil
}

func (c *SimpleClient) GetPeerMgr() PeerMgr {
	return c.peerMgr
}

func (c *SimpleClient) GetPackHeaderFactory() PackHeaderFactory {
	return c.headerFactory
}

func (s *BaseServer) Start() {
	s.peerMgr.Start()
}

func (s *BaseServer) Stop() {
	s.peerMgr.Stop()
}
