// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"errors"
	"sync"
	"time"

	"github.com/yxlib/yx"
)

var (
	ErrPeerMgrHeaderNil    = errors.New("header is nil")
	ErrPeerMgrPeerNotExist = errors.New("peer not exist")
	ErrPeerMgrInvalidPack  = errors.New("invalid pack")
)

const (
	PEER_MGR_MAX_BUFF_REUSE_CNT   = 1024
	PEER_MGR_READ_WAIT_DURATION   = 5 * time.Millisecond
	PEER_MGR_CHECK_CLOSE_INTERVAL = time.Second
	PEER_MGR_MAX_PEER_NO          = 100000
)

//========================
//       PackWrap
//========================
type PackWrap struct {
	Pack           *Pack
	DirectPeerType uint32 // maybe translate by a middle server
	DirectPeerNo   uint32
}

func NewPackWrap(pack *Pack, directPeerType uint32, directPeerNo uint32) *PackWrap {
	return &PackWrap{
		Pack:           pack,
		DirectPeerType: directPeerType,
		DirectPeerNo:   directPeerNo,
	}
}

//========================
//       PeerMgr
//========================
type PeerMgr interface {
	AddListener(l P2pNetListener)
	RemoveListener(l P2pNetListener)
	Start()
	Stop()
	AddPeer(peer *Peer, bUnknownPeer bool, bRegister bool)
	GetPeerId(peerType uint32, peerNo uint32) uint32
	GetPeer(peerType uint32, peerNo uint32) (*Peer, bool)
	IsPeerExist(peerType uint32, peerNo uint32) bool
	Send(pack *Pack) error
	SendByPeer(pack *Pack, nextPeerType uint32, nextPeerNo uint32) error
	CloseRead(peerType uint32, peerNo uint32)
	ClosePeer(peerType uint32, peerNo uint32)
	ReusePack(pack *Pack, peerType uint32, peerNo uint32)
}

type BasePeerMgr struct {
	peerType       uint32
	peerNo         uint32
	buffFactory    *yx.BuffFactory
	lckReadPeerIds *sync.Mutex
	readPeerIdSet  map[uint32]bool
	lckPeer        *sync.RWMutex
	mapPeerId2Peer map[uint32]*Peer
	unknownPeers   []*Peer
	closedPeers    []*Peer

	topPriorityListeners []P2pNetListener
	listeners            []P2pNetListener
	lckListeners         *sync.RWMutex

	evtStop *yx.Event
	evtExit *yx.Event

	lckRunning *sync.Mutex
	bRunning   bool

	ec     *yx.ErrCatcher
	logger *yx.Logger
}

func NewBasePeerMgr(peerType uint32, peerNo uint32) *BasePeerMgr {
	return &BasePeerMgr{
		peerType:             peerType,
		peerNo:               peerNo,
		buffFactory:          yx.NewBuffFactory(),
		lckReadPeerIds:       &sync.Mutex{},
		readPeerIdSet:        make(map[uint32]bool),
		lckPeer:              &sync.RWMutex{},
		mapPeerId2Peer:       make(map[uint32]*Peer),
		unknownPeers:         make([]*Peer, 0),
		closedPeers:          make([]*Peer, 0),
		topPriorityListeners: make([]P2pNetListener, 0),
		listeners:            make([]P2pNetListener, 0),
		lckListeners:         &sync.RWMutex{},
		evtStop:              yx.NewEvent(),
		evtExit:              yx.NewEvent(),
		lckRunning:           &sync.Mutex{},
		bRunning:             false,
		ec:                   yx.NewErrCatcher("p2pnet.BasePeerMgr"),
		logger:               yx.NewLogger("p2pnet.BasePeerMgr"),
	}
}

func (m *BasePeerMgr) AddTopPriorityListener(l P2pNetListener) {
	if l == nil {
		return
	}

	m.lckListeners.Lock()
	defer m.lckListeners.Unlock()

	m.topPriorityListeners = append(m.topPriorityListeners, l)
}

func (m *BasePeerMgr) RemoveTopPriorityListener(l P2pNetListener) {
	if l == nil {
		return
	}

	m.lckListeners.Lock()
	defer m.lckListeners.Unlock()

	for i, exist := range m.topPriorityListeners {
		if exist == l {
			m.topPriorityListeners = append(m.topPriorityListeners[:i], m.topPriorityListeners[i+1:]...)
			break
		}
	}
}

func (m *BasePeerMgr) AddListener(l P2pNetListener) {
	if l == nil {
		return
	}

	m.lckListeners.Lock()
	defer m.lckListeners.Unlock()

	m.listeners = append(m.listeners, l)
}

func (m *BasePeerMgr) RemoveListener(l P2pNetListener) {
	if l == nil {
		return
	}

	m.lckListeners.Lock()
	defer m.lckListeners.Unlock()

	for i, exist := range m.listeners {
		if exist == l {
			m.listeners = append(m.listeners[:i], m.listeners[i+1:]...)
			break
		}
	}
}

func (m *BasePeerMgr) Start() {
	bUpdate := m.setRunning(true)
	if !bUpdate {
		return
	}

	m.loop()
}

func (m *BasePeerMgr) Stop() {
	bUpdate := m.setRunning(false)
	if !bUpdate {
		return
	}

	m.evtStop.Send()
	m.evtExit.Wait()

	m.closeAllPeers()
	m.waitAllPeerClosed()
}

func (m *BasePeerMgr) AddPeer(peer *Peer, bUnknownPeer bool, bRegister bool) {
	peer.SetMgr(m)
	peer.SetBuffFactory(m.buffFactory)

	if bUnknownPeer {
		bSucc := m.addUnknownPeer(peer)
		if bSucc {
			peer.Open()
		}

	} else {
		peerType := peer.GetPeerType()
		peerNo := peer.GetPeerNo()
		bSucc := m.addKnownPeer(peer, peerType, peerNo)
		if bSucc {
			peer.Open()
			if bRegister {
				m.sendRegPack(peer)
			}

			m.notifyPeerOpen(peerType, peerNo)
		}
	}
}

func (m *BasePeerMgr) GetPeerId(peerType uint32, peerNo uint32) uint32 {
	return peerType*PEER_MGR_MAX_PEER_NO + peerNo
}

func (m *BasePeerMgr) GetPeer(peerType uint32, peerNo uint32) (*Peer, bool) {
	peer, ok := m.getPeerImpl(peerType, peerNo)
	return peer, ok
}

func (m *BasePeerMgr) IsPeerExist(peerType uint32, peerNo uint32) bool {
	_, ok := m.getPeerImpl(peerType, peerNo)
	return ok
}

func (m *BasePeerMgr) Send(pack *Pack) error {
	dstType, dstNo := pack.Header.GetDstPeer()
	err := m.SendByPeer(pack, dstType, dstNo)
	return m.ec.Throw("Send", err)
}

func (m *BasePeerMgr) SendByPeer(pack *Pack, nextPeerType uint32, nextPeerNo uint32) error {
	var err error = nil
	defer m.ec.DeferThrow("SendByPeer", &err)

	if pack.Header == nil {
		err = ErrPeerMgrHeaderNil
		return err
	}

	peer, ok := m.getPeerImpl(nextPeerType, nextPeerNo)
	if !ok {
		err = ErrPeerMgrPeerNotExist
		return err
	}

	err = peer.PushWritePack(pack)
	return err
}

func (m *BasePeerMgr) CloseRead(peerType uint32, peerNo uint32) {
	peer, ok := m.getPeerImpl(peerType, peerNo)
	if ok {
		peer.CloseRead()
	}
}

func (m *BasePeerMgr) ClosePeer(peerType uint32, peerNo uint32) {
	peer, ok := m.getPeerImpl(peerType, peerNo)
	if ok {
		m.logger.W("ClosePeer")
		peer.Close()
	}
}

func (m *BasePeerMgr) ReusePack(pack *Pack, peerType uint32, peerNo uint32) {
	peer, ok := m.getPeerImpl(peerType, peerNo)
	if ok {
		peer.ReusePack(pack)
	}
}

//========================
//       PeerListener
//========================
func (m *BasePeerMgr) GetOwnerType() uint32 {
	return m.peerType
}

func (m *BasePeerMgr) GetOwnerNo() uint32 {
	return m.peerNo
}

func (m *BasePeerMgr) OnPeerError(p *Peer, err error) {
	m.notifyPeerError(p, err)
}

func (m *BasePeerMgr) OnPeerClose(p *Peer) {
	m.closePeerImpl(p)
}

func (m *BasePeerMgr) OnPeerRead(p *Peer) {
	ok := m.isUnknownPeer(p)
	if ok {
		m.handleUnknownPeerRead(p)
		return
	}

	m.recordRead(p)
}

//========================
//       private
//========================
func (m *BasePeerMgr) setRunning(bRunning bool) (bUpdate bool) {
	m.lckRunning.Lock()
	defer m.lckRunning.Unlock()

	if m.bRunning == bRunning {
		return false
	}

	m.bRunning = bRunning
	return true
}

func (m *BasePeerMgr) handleUnknownPeerRead(p *Peer) {
	pack, err := p.PopReadPack()
	if err != nil {
		return
	}

	if pack.Header.IsDataPack() {
		m.logger.E("unknown peer receive a data pack")
		p.Close()
		return
	}

	err = m.handleCtrlPack(pack, p, true)
	if err != nil {
		m.logger.E("unknown peer handle control package err: ", err)
		p.Close()
	}
}

func (m *BasePeerMgr) recordRead(p *Peer) {
	m.lckReadPeerIds.Lock()
	defer m.lckReadPeerIds.Unlock()

	peerType := p.GetPeerType()
	peerNo := p.GetPeerNo()
	id := m.GetPeerId(peerType, peerNo)
	m.readPeerIdSet[id] = true
}

func (m *BasePeerMgr) popReadPeerIds() []uint32 {
	m.lckReadPeerIds.Lock()
	defer m.lckReadPeerIds.Unlock()

	setLen := len(m.readPeerIdSet)
	if setLen == 0 {
		return []uint32{}
	}

	ids := make([]uint32, 0, setLen)
	for id := range m.readPeerIdSet {
		ids = append(ids, id)
	}

	m.readPeerIdSet = make(map[uint32]bool)

	return ids
}

func (m *BasePeerMgr) loop() {
	for {
		select {
		case <-m.evtStop.C:
			goto Exit0

		default:
			m.cleanClosedPeers()
			m.handleReadPacks()
		}
	}

Exit0:
	m.evtExit.Send()
}

func (m *BasePeerMgr) handleReadPacks() {
	if m.getListenerCnt() == 0 {
		return
	}

	ids := m.popReadPeerIds()
	if len(ids) == 0 {
		<-time.After(PEER_MGR_READ_WAIT_DURATION)
		return
	}

	packs := m.popReadPacks(ids)
	if len(packs) == 0 {
		return
	}

	for _, pack := range packs {
		m.notifyPeerRead(pack.Pack, pack.DirectPeerType, pack.DirectPeerNo)
	}
}

func (m *BasePeerMgr) popReadPacks(ids []uint32) []*PackWrap {
	var loopPacks []*PackWrap = nil
	packs := make([]*PackWrap, 0, len(ids))
	remainPeers := m.getPeersImpl(ids)

	for {
		loopPacks, remainPeers = m.loopReadPack(remainPeers)
		if len(loopPacks) > 0 {
			packs = append(packs, loopPacks...)
		}

		if len(remainPeers) == 0 {
			break
		}
	}

	return packs
}

func (m *BasePeerMgr) loopReadPack(peers []*Peer) ([]*PackWrap, []*Peer) {
	packs := make([]*PackWrap, 0, len(peers))
	remainPeers := make([]*Peer, 0, len(peers))

	for _, peer := range peers {
		pack, err := peer.PopReadPack()
		if err != nil {
			continue
		}

		if !pack.Header.IsDataPack() {
			err = m.handleCtrlPack(pack, peer, false)
			if err != nil {
				m.logger.E("known peer handle control package err: ", err)
				peer.Close()
				continue
			}

		} else {
			// TODO record act
			peerType := peer.GetPeerType()
			peerNo := peer.GetPeerNo()
			wrap := NewPackWrap(pack, peerType, peerNo)
			packs = append(packs, wrap)
		}

		remainPeers = append(remainPeers, peer)
	}

	return packs, remainPeers
}

func (m *BasePeerMgr) handleCtrlPack(pack *Pack, peer *Peer, bUnknownPeer bool) error {
	var err error = nil
	defer m.ec.Catch("handleCtrlPack", &err)

	if pack.Header.IsPongPack() {
		return nil
	}

	// TODO record act

	// factory := peer.GetHeaderFactory()
	if pack.Header.IsPingPack() {
		m.handlePingPack(peer)
		return nil
	}

	if pack.Header.IsRegisterReqPack() {
		if !bUnknownPeer {
			err = ErrPeerMgrInvalidPack
			return err
		}

		m.handleRegReq(pack, peer)
		return nil
	}

	if pack.Header.IsRegisterRespPack() {
		if bUnknownPeer {
			err = ErrPeerMgrInvalidPack
			return err
		}

		// m.bindPeer(peer)
		return nil
	}

	err = ErrPeerMgrInvalidPack
	return err
}

func (m *BasePeerMgr) handlePingPack(peer *Peer) {
	pongPack := peer.CreatePack()
	pongPack.Header.SetPongPack()
	go peer.PushWritePack(pongPack)
}

func (m *BasePeerMgr) sendRegPack(peer *Peer) {
	peerType := m.peerType
	peerNo := m.peerNo

	regPack := peer.CreatePack()
	header := regPack.Header
	header.SetRegisterReqPack()
	header.SetHasSrc(true)
	header.SetSrcPeer(peerType, peerNo)
	peer.PushWritePack(regPack)
}

func (m *BasePeerMgr) handleRegReq(pack *Pack, peer *Peer) {
	srcPeerType, srcPeerNo := pack.Header.GetSrcPeer()
	peer.SetPeerTypeAndNo(srcPeerType, srcPeerNo)
	m.bindPeer(peer)

	respPack := peer.CreatePack()
	respPack.Header.SetRegisterRespPack()
	peer.PushWritePack(respPack)
}

func (m *BasePeerMgr) addKnownPeer(peer *Peer, peerType uint32, peerNo uint32) bool {
	m.lckPeer.Lock()
	defer m.lckPeer.Unlock()

	id := m.GetPeerId(peerType, peerNo)
	oldPeer, ok := m.mapPeerId2Peer[id]
	if ok {
		if oldPeer == peer {
			return false
		}

		m.logger.E("add known peer close old peer (", peerType, ", ", peerNo, ")")
		oldPeer.Close()
	}

	m.mapPeerId2Peer[id] = peer
	return true
}

func (m *BasePeerMgr) addUnknownPeer(peer *Peer) bool {
	m.lckPeer.Lock()
	defer m.lckPeer.Unlock()

	for _, existPeer := range m.unknownPeers {
		if existPeer == peer {
			return false
		}
	}

	m.unknownPeers = append(m.unknownPeers, peer)
	return true
}

func (m *BasePeerMgr) bindPeer(peer *Peer) {
	m.bindPeerImpl(peer)

	peerType := peer.GetPeerType()
	peerNo := peer.GetPeerNo()
	m.notifyPeerOpen(peerType, peerNo)
}

func (m *BasePeerMgr) bindPeerImpl(peer *Peer) {
	m.lckPeer.Lock()
	defer m.lckPeer.Unlock()

	for i, existPeer := range m.unknownPeers {
		if existPeer == peer {
			m.unknownPeers = append(m.unknownPeers[:i], m.unknownPeers[i+1:]...)
			break
		}
	}

	id := m.GetPeerId(peer.peerType, peer.peerNo)
	oldPeer, ok := m.mapPeerId2Peer[id]
	if ok && (oldPeer != peer) {
		m.logger.E("bind peer close old peer (", peer.peerType, ", ", peer.peerNo, ")")
		oldPeer.Close()
	}

	m.mapPeerId2Peer[id] = peer
}

func (m *BasePeerMgr) getPeerImpl(peerType uint32, peerNo uint32) (*Peer, bool) {
	m.lckPeer.RLock()
	defer m.lckPeer.RUnlock()

	id := m.GetPeerId(peerType, peerNo)
	peer, ok := m.mapPeerId2Peer[id]
	return peer, ok
}

func (m *BasePeerMgr) getPeersImpl(ids []uint32) []*Peer {
	m.lckPeer.RLock()
	defer m.lckPeer.RUnlock()

	peers := make([]*Peer, 0, len(ids))
	for _, id := range ids {
		peer, ok := m.mapPeerId2Peer[id]
		if ok {
			peers = append(peers, peer)
		}
	}

	return peers
}

func (m *BasePeerMgr) isUnknownPeer(p *Peer) bool {
	m.lckPeer.RLock()
	defer m.lckPeer.RUnlock()

	for _, existPeer := range m.unknownPeers {
		if existPeer == p {
			return true
		}
	}

	return false
}

func (m *BasePeerMgr) closePeerImpl(p *Peer) {
	m.lckPeer.Lock()
	defer m.lckPeer.Unlock()

	for i, existPeer := range m.unknownPeers {
		if existPeer == p {
			m.unknownPeers = append(m.unknownPeers[:i], m.unknownPeers[i+1:]...)
			m.closedPeers = append(m.closedPeers, p)
			return
		}
	}

	peerType := p.GetPeerType()
	peerNo := p.GetPeerNo()
	id := m.GetPeerId(peerType, peerNo)
	_, ok := m.mapPeerId2Peer[id]
	if ok {
		delete(m.mapPeerId2Peer, id)
		m.closedPeers = append(m.closedPeers, p)
	}
}

func (m *BasePeerMgr) cleanClosedPeers() int {
	removePeers, leftCnt := m.removeClosedPeers()

	for _, p := range removePeers {
		peerType := p.GetPeerType()
		peerNo := p.GetPeerNo()
		ipAddr := p.GetIpAddr()
		m.notifyPeerClose(peerType, peerNo, ipAddr)
	}

	return leftCnt
}

func (m *BasePeerMgr) removeClosedPeers() ([]*Peer, int) {
	m.lckPeer.Lock()
	defer m.lckPeer.Unlock()

	removePeers := make([]*Peer, 0)
	for i := len(m.closedPeers) - 1; i >= 0; i-- {
		peer := m.closedPeers[i]
		if !peer.IsExit() {
			continue
		}

		// if peer.IsPeerNoReuse() {
		// 	m.serv.ReuseConnPeerNo(peer.GetPeerNo())
		// }

		// if m.cfg.IpConnCntLimit > 0 {
		// 	ipAddr := peer.GetIpAddr()
		// 	m.reduceIpConnCnt(ipAddr)
		// }

		removePeers = append(removePeers, peer)
		m.closedPeers = append(m.closedPeers[:i], m.closedPeers[i+1:]...)
	}

	return removePeers, len(m.closedPeers)
}

func (m *BasePeerMgr) closeAllPeers() {
	m.lckPeer.Lock()
	defer m.lckPeer.Unlock()

	// unknown peers
	for _, peer := range m.unknownPeers {
		peer.Close()
	}

	// m.closedPeers = append(m.closedPeers, m.unknownPeers...)

	// normal peers
	for _, peer := range m.mapPeerId2Peer {
		peer.Close()
		// m.closedPeers = append(m.closedPeers, peer)
	}
}

func (m *BasePeerMgr) waitAllPeerClosed() {
	ticker := time.NewTicker(PEER_MGR_CHECK_CLOSE_INTERVAL)

	for {
		<-ticker.C
		cnt := m.cleanClosedPeers()
		if cnt == 0 {
			break
		}
	}

	ticker.Stop()
}

func (m *BasePeerMgr) getTopPriorityListenerCnt() int {
	m.lckListeners.RLock()
	defer m.lckListeners.RUnlock()

	return len(m.topPriorityListeners)
}

func (m *BasePeerMgr) getListenerCnt() int {
	m.lckListeners.RLock()
	defer m.lckListeners.RUnlock()

	return len(m.listeners)
}

func (m *BasePeerMgr) notifyPeerOpen(peerType uint32, peerNo uint32) {
	m.lckListeners.RLock()
	defer m.lckListeners.RUnlock()

	for _, l := range m.topPriorityListeners {
		l.OnP2pNetOpenPeer(m, peerType, peerNo)
	}

	for _, l := range m.listeners {
		l.OnP2pNetOpenPeer(m, peerType, peerNo)
	}
}

func (m *BasePeerMgr) notifyPeerClose(peerType uint32, peerNo uint32, ipAddr string) {
	m.lckListeners.RLock()
	defer m.lckListeners.RUnlock()

	for _, l := range m.topPriorityListeners {
		l.OnP2pNetClosePeer(m, peerType, peerNo, ipAddr)
	}

	for _, l := range m.listeners {
		l.OnP2pNetClosePeer(m, peerType, peerNo, ipAddr)
	}
}

func (m *BasePeerMgr) notifyPeerRead(pack *Pack, recvPeerType uint32, recvPeerNo uint32) {
	m.lckListeners.RLock()
	defer m.lckListeners.RUnlock()

	for _, l := range m.topPriorityListeners {
		bHandle := l.OnP2pNetReadPack(m, pack, recvPeerType, recvPeerNo)
		if bHandle {
			return
		}
	}

	for _, l := range m.listeners {
		bHandle := l.OnP2pNetReadPack(m, pack, recvPeerType, recvPeerNo)
		if bHandle {
			break
		}
	}
}

func (m *BasePeerMgr) notifyPeerError(p *Peer, err error) {
	m.lckListeners.RLock()
	defer m.lckListeners.RUnlock()

	for _, l := range m.topPriorityListeners {
		l.OnP2pNetError(m, p.GetPeerType(), p.GetPeerNo(), err)
	}

	for _, l := range m.listeners {
		l.OnP2pNetError(m, p.GetPeerType(), p.GetPeerNo(), err)
	}
}
