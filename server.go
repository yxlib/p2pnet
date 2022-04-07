// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"net"

	"github.com/yxlib/yx"
)

type Server interface {
	SetAcceptBindPeerType(bindPeerType uint32, minPeerNo uint64, maxPeerNo uint64)
	Listen(network string, address string) error
	Close() error
}

type BaseServer struct {
	peerMgr PeerMgr

	ipConnCntLimit uint32
	mapIp2ConnCnt  map[string]uint32

	bAcceptBindPeerType bool
	bindPeerType        uint32
	peerNoGenerator     *yx.IdGenerator
	headerFactory       PackHeaderFactory
	maxReadQue          uint32
	maxWriteQue         uint32

	ec *yx.ErrCatcher
}

func NewBaseServ(peerMgr PeerMgr, ipConnCntLimit uint32, headerFactory PackHeaderFactory, maxReadQue uint32, maxWriteQue uint32) *BaseServer {
	s := &BaseServer{
		peerMgr:             peerMgr,
		ipConnCntLimit:      ipConnCntLimit,
		mapIp2ConnCnt:       make(map[string]uint32),
		bAcceptBindPeerType: false,
		bindPeerType:        0,
		peerNoGenerator:     nil,
		headerFactory:       headerFactory,
		maxReadQue:          maxReadQue,
		maxWriteQue:         maxWriteQue,
		ec:                  yx.NewErrCatcher("p2pnet.BaseServer"),
	}

	if s.peerMgr != nil {
		s.peerMgr.AddListener(s)
	}
	return s
}

func (s *BaseServer) GetPeerMgr() PeerMgr {
	return s.peerMgr
}

func (s *BaseServer) SetAcceptBindPeerType(bindPeerType uint32, minPeerNo uint64, maxPeerNo uint64) {
	s.bAcceptBindPeerType = true
	s.bindPeerType = bindPeerType
	s.peerNoGenerator = yx.NewIdGenerator(minPeerNo, maxPeerNo)
}

func (s *BaseServer) Listen(network string, address string) error {
	return nil
}

func (s *BaseServer) Close() error {
	return nil
}

func (s *BaseServer) HandleIpLimit(c net.Conn) bool {
	ipAddr := GetRemoteAddr(c)
	if s.isIpConnReachLimit(ipAddr) {
		s.ec.Catch("handleIpLimit", &ErrSockServIpConnLimit)
		c.Close()
		return true
	}

	s.addIpConnCnt(ipAddr)
	return false
}

func (s *BaseServer) OpenConn(c net.Conn) {
	var err error = nil
	peerNo := uint64(0)
	if s.bAcceptBindPeerType {
		peerNo, err = s.peerNoGenerator.GetId()
		if err != nil {
			s.ec.Catch("OpenConn", &err)
			c.Close()
			return
		}

		s.openPeer(s.bindPeerType, uint32(peerNo), c, false)
	} else {
		s.openPeer(0, 0, c, true)
	}
}

//========================
//     P2pNetListener
//========================
func (s *BaseServer) OnP2pNetOpenPeer(m PeerMgr, peerType uint32, peerNo uint32) {
}

func (s *BaseServer) OnP2pNetClosePeer(m PeerMgr, peerType uint32, peerNo uint32, ipAddr string) {
	s.reduceIpConnCnt(ipAddr)

	if s.bAcceptBindPeerType && (peerType == s.bindPeerType) {
		s.peerNoGenerator.ReuseId(uint64(peerNo))
	}
}

func (s *BaseServer) OnP2pNetReadPack(m PeerMgr, pack *Pack, recvPeerType uint32, recvPeerNo uint32) bool {
	return false
}

func (s *BaseServer) OnP2pNetError(m PeerMgr, peerType uint32, peerNo uint32, err error) {
}

//========================
//       private
//========================
func (s *BaseServer) isIpConnReachLimit(ipAddr string) bool {
	if s.ipConnCntLimit == 0 {
		return false
	}

	cnt, ok := s.mapIp2ConnCnt[ipAddr]
	if !ok {
		return false
	}

	return cnt >= s.ipConnCntLimit
}

func (s *BaseServer) addIpConnCnt(ipAddr string) {
	if s.ipConnCntLimit == 0 {
		return
	}

	_, ok := s.mapIp2ConnCnt[ipAddr]
	if !ok {
		s.mapIp2ConnCnt[ipAddr] = 0
	}

	s.mapIp2ConnCnt[ipAddr]++
}

func (s *BaseServer) reduceIpConnCnt(ipAddr string) {
	if s.ipConnCntLimit == 0 {
		return
	}

	_, ok := s.mapIp2ConnCnt[ipAddr]
	if !ok || (s.mapIp2ConnCnt[ipAddr] == 0) {
		return
	}

	s.mapIp2ConnCnt[ipAddr]--
}

func (s *BaseServer) openPeer(peerType uint32, peerNo uint32, c net.Conn, bUnknownPeer bool) {
	p := NewPeer(peerType, peerNo, c, s.maxReadQue, s.maxWriteQue)
	p.SetHeaderFactory(s.headerFactory)
	s.peerMgr.AddPeer(p, bUnknownPeer, false)
}
