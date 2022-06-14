// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import "github.com/yxlib/yx"

//========================
//      P2pNetListener
//========================
type P2pNetListener interface {
	OnP2pNetOpenPeer(m PeerMgr, peerType uint32, peerNo uint32)
	OnP2pNetClosePeer(m PeerMgr, peerType uint32, peerNo uint32, ipAddr string)
	OnP2pNetReadPack(m PeerMgr, pack *Pack, recvPeerType uint32, recvPeerNo uint32) bool
	OnP2pNetError(m PeerMgr, peerType uint32, peerNo uint32, err error)
}

type BaseP2pNetListner struct {
}

func NewBaseP2pNetListner() *BaseP2pNetListner {
	return &BaseP2pNetListner{}
}

func (n *BaseP2pNetListner) OnP2pNetOpenPeer(m PeerMgr, peerType uint32, peerNo uint32) {
}

func (n *BaseP2pNetListner) OnP2pNetClosePeer(m PeerMgr, peerType uint32, peerNo uint32, ipAddr string) {
}

func (n *BaseP2pNetListner) OnP2pNetReadPack(m PeerMgr, pack *Pack, recvPeerType uint32, recvPeerNo uint32) bool {
	return false
}

func (n *BaseP2pNetListner) OnP2pNetError(m PeerMgr, peerType uint32, peerNo uint32, err error) {
}

//========================
//     LogNetListener
//========================
type LogNetListener struct {
	logger *yx.Logger
	ec     *yx.ErrCatcher
}

func NewLogNetListener() *LogNetListener {
	return &LogNetListener{
		logger: yx.NewLogger("P2pNetListener"),
		ec:     yx.NewErrCatcher("P2pNetListener"),
	}
}

func (l *LogNetListener) OnP2pNetOpenPeer(m PeerMgr, peerType uint32, peerNo uint32) {
	l.logger.I("OnP2pNetOpenPeer (", peerType, ", ", peerNo, ")")
}

func (l *LogNetListener) OnP2pNetClosePeer(m PeerMgr, peerType uint32, peerNo uint32, ipAddr string) {
	l.logger.I("OnP2pNetClosePeer (", peerType, ", ", peerNo, ")")
}

func (l *LogNetListener) OnP2pNetReadPack(m PeerMgr, pack *Pack, recvPeerType uint32, recvPeerNo uint32) bool {
	l.logger.I("OnP2pNetReadPack (", recvPeerType, ", ", recvPeerNo, ")")
	return false
}

func (l *LogNetListener) OnP2pNetError(m PeerMgr, peerType uint32, peerNo uint32, err error) {
	l.logger.E("OnP2pNetError (", peerType, ", ", peerNo, "), err: ", err)
	l.ec.Catch("OnP2pNetError", &err)
}
