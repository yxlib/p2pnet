// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yxlib/yx"
)

var (
	ErrPeerReadQueEmpty     = errors.New("read queue empty")
	ErrPeerReadQueClose     = errors.New("read queue close")
	ErrPeerWritePackNil     = errors.New("write pack is nil")
	ErrPeerHeaderFactoryNil = errors.New("header factory is nil")
	ErrPeerReadStepErr      = errors.New("unknown read step")
	ErrPeerMarkErr          = errors.New("mark error")
	ErrPeerReadPackTooBig   = errors.New("package too big")
	ErrPeerCloseRead        = errors.New("peer close read")
	ErrPeerClose            = errors.New("peer close")
	ErrPeerEmptyPack        = errors.New("empty pack")
	ErrBadPeer              = errors.New("bad peer")
)

const (
	PEER_READ_TIMEOUT   = time.Second * 3
	PEER_WRITE_TIMEOUT  = time.Second * 3
	PEER_READ_BUFF_SIZE = 4 * 1024
)

const (
	PEER_STAT_OPEN uint8 = iota
	PEER_STAT_CLOSE_READ
	PEER_STAT_ERROR
	PEER_STAT_CLOSE_WRITE
	PEER_STAT_CLOSE_CONN
	PEER_STAT_EXIT
)

const (
	PEER_READ_STEP_UNKNOWN int = iota
	PEER_READ_STEP_MARK
	PEER_READ_STEP_HEADER
	PEER_READ_STEP_DATA
	PEER_READ_STEP_END
)

// type ActMode uint8

// const (
// 	ACT_MODE_NONE ActMode = iota
// 	ACT_MODE_INITIATIVE
// 	ACT_MODE_PASSIVE
// )

type HealtyCfg struct {
	MaxIdleTime int64 // nanoseconds
	MaxActIntv  int64 // nanoseconds
	MaxSendFreq int   // second
	MaxSendUnit int   // second
}

type PeerListener interface {
	GetOwnerType() uint32
	GetOwnerNo() uint32
	OnPeerError(p *Peer, err error)
	OnPeerClose(p *Peer)
	OnPeerRead(p *Peer)
}

type Peer struct {
	peerType uint32
	peerNo   uint32
	conn     net.Conn
	ipAddr   string
	stat     uint8
	lckStat  *sync.RWMutex

	mgr         PeerListener
	packPool    *PackPool
	buffFactory *yx.BuffFactory
	// headerFactory PackHeaderFactory
	bSingleBuffPack bool

	maxPayload     int
	wantReadLen    int
	readBuff       *yx.SimpleBuffer
	evtExitRead    *yx.Event
	chanCloseWrite chan byte
	queReadPacks   chan *Pack
	queWritePacks  chan *Pack

	connStartTime int64
	bHasDataOpt   bool
	actTime       int64
	bStatSendInfo bool
	lckSendInfo   *sync.Mutex
	sendCnt       int
	sendSize      int

	ec     *yx.ErrCatcher
	logger *yx.Logger
}

func NewPeer(peerType uint32, peerNo uint32, c net.Conn, maxReadQue uint32, maxWriteQue uint32) *Peer {
	return &Peer{
		peerType:    peerType,
		peerNo:      peerNo,
		conn:        c,
		ipAddr:      GetRemoteAddr(c),
		stat:        PEER_STAT_OPEN,
		lckStat:     &sync.RWMutex{},
		mgr:         nil,
		packPool:    nil,
		buffFactory: nil,
		// headerFactory: nil,
		bSingleBuffPack: false,

		maxPayload:     PACK_MAX_PAYLOAD,
		wantReadLen:    0,
		readBuff:       yx.NewSimpleBuffer(PEER_READ_BUFF_SIZE),
		evtExitRead:    yx.NewEvent(),
		chanCloseWrite: make(chan byte),
		queReadPacks:   make(chan *Pack, maxReadQue),
		queWritePacks:  make(chan *Pack, maxWriteQue),
		connStartTime:  0,
		bHasDataOpt:    false,
		actTime:        0,
		bStatSendInfo:  false,
		lckSendInfo:    &sync.Mutex{},
		sendCnt:        0,
		sendSize:       0,
		ec:             yx.NewErrCatcher("p2pnet.Peer"),
		logger:         yx.NewLogger("p2pnet.Peer"),
	}
}

func (p *Peer) SetMgr(mgr PeerListener) {
	p.mgr = mgr
}

func (p *Peer) SetPackPool(pool *PackPool) {
	p.packPool = pool
}

func (p *Peer) CreatePack() *Pack {
	return p.packPool.Get()
}

func (p *Peer) ReusePack(pack *Pack) {
	for _, buff := range pack.oriBuffs {
		p.buffFactory.ReuseBuff(&buff)
	}

	p.packPool.Put(pack)
}

func (p *Peer) SetBuffFactory(pool *yx.BuffFactory) {
	p.buffFactory = pool
}

// func (p *Peer) SetHeaderFactory(headerFactory PackHeaderFactory) {
// 	p.headerFactory = headerFactory
// }

// func (p *Peer) GetHeaderFactory() PackHeaderFactory {
// 	return p.headerFactory
// }

func (p *Peer) SetPeerTypeAndNo(peerType uint32, peerNo uint32) {
	p.peerType = peerType
	p.peerNo = peerNo
}

func (p *Peer) GetPeerTypeAndNo() (uint32, uint32) {
	return p.peerType, p.peerNo
}

func (p *Peer) GetPeerType() uint32 {
	return p.peerType
}

func (p *Peer) GetPeerNo() uint32 {
	return p.peerNo
}

func (p *Peer) GetIpAddr() string {
	return p.ipAddr
}

func (p *Peer) SetSingleBuffPack() {
	p.bSingleBuffPack = true
}

func (p *Peer) SetMaxPayload(maxPayload int) {
	p.maxPayload = maxPayload
}

func (p *Peer) Open() {
	p.connStartTime = time.Now().UnixNano()

	go p.readLoop()
	go p.writeLoop()
}

func (p *Peer) CloseRead() {
	p.setStat(PEER_STAT_CLOSE_READ)
}

func (p *Peer) Close() {
	p.lckStat.Lock()
	defer p.lckStat.Unlock()

	p.closeWrite()
}

func (p *Peer) ForceClose() {
	p.lckStat.Lock()
	defer p.lckStat.Unlock()

	p.closeWrite()
	p.closeConn()
}

func (p *Peer) IsExit() bool {
	return p.getStat() == PEER_STAT_EXIT
}

func (p *Peer) PopReadPack() (*Pack, error) {
	if len(p.queReadPacks) == 0 {
		return nil, p.ec.Throw("PopReadPack", ErrPeerReadQueEmpty)
	}

	pack, ok := <-p.queReadPacks
	if !ok {
		return nil, p.ec.Throw("PopReadPack", ErrPeerReadQueClose)
	}

	return pack, nil
}

func (p *Peer) PushWritePack(pack *Pack) error {
	if nil == pack {
		return p.ec.Throw("PushWritePack", ErrPeerWritePackNil)
	}

	select {
	case <-p.chanCloseWrite:
		return p.ec.Throw("PushWritePack", ErrPeerClose)
	default:
		p.queWritePacks <- pack
	}

	return nil
}

func (p *Peer) SetStatSendInfo() {
	p.bStatSendInfo = true
}

func (p *Peer) CheckHealthy(cfg *HealtyCfg, startTimeNanosec int64, nowNanosec int64) {
	if p.isBadPeer(cfg, startTimeNanosec, nowNanosec) {
		p.notifyError(ErrBadPeer)
	}
}

func (p *Peer) readLoop() {
	var pack *Pack = nil
	var err error = nil

	for {
		pack, err = p.readPack()
		if err != nil {
			break
		}

		p.recordHealthyInfo(pack)

		p.queReadPacks <- pack
		p.mgr.OnPeerRead(p)
	}

	if p.getStat() == PEER_STAT_OPEN {
		p.notifyError(err)
	}

	// close(p.queReadPacks)

	// // wake up write loop and note to exit
	// close(p.chanCloseWrite)
	// // close(p.queWritePacks)

	p.evtExitRead.Send()
}

func (p *Peer) writeLoop() {
	var err error = nil
	var bExit bool = false

	for {
		bExit, err = p.selectOneWritePack()
		if err != nil || bExit {
			break
		}
	}

	// write error, cause force close later
	if p.getStat() < PEER_STAT_ERROR && err != nil {
		p.notifyError(err)
	} else { // wake up read loop and note to exit at once
		p.closeConnInner()
		// p.Close()
	}

	p.evtExitRead.Wait()

	p.mgr.OnPeerClose(p)
	p.setStat(PEER_STAT_EXIT)
	// p.bExit = true
}

func (p *Peer) selectOneWritePack() (bool, error) {
	var err error = nil
	defer p.ec.DeferThrow("selectOneWritePack", &err)

	select {
	case pack := <-p.queWritePacks:
		err = p.writeOnePack(pack)

	case <-p.chanCloseWrite:
		select {
		case pack := <-p.queWritePacks:
			err = p.writeOnePack(pack)

		default:
			return true, nil
		}
	}

	return (err != nil), err
}

func (p *Peer) writeOnePack(pack *Pack) error {
	err := p.writePack(pack)
	if err != nil {
		return p.ec.Throw("writeOnePack", err)
	}

	p.ReusePack(pack)
	return nil
}

func (p *Peer) readPack() (*Pack, error) {
	var err error = nil
	defer p.ec.DeferThrow("readPack", &err)
	// header := p.headerFactory.CreateHeader()
	// pack := NewPack(header)
	pack := p.CreatePack()
	step := PEER_READ_STEP_MARK
	p.wantReadLen = pack.Header.GetMarkLen()

	for {
		step, err = p.readPackPart(pack, step)
		if err != nil {
			break
		}

		if step == PEER_READ_STEP_END {
			break
		}
	}

	if err != nil {
		p.ReusePack(pack)
		return nil, err
	}

	return pack, nil
}

func (p *Peer) readPackPart(pack *Pack, step int) (int, error) {
	var err error = nil
	defer p.ec.DeferThrow("readPackPart", &err)

	nextStep := step
	if step == PEER_READ_STEP_MARK || step == PEER_READ_STEP_HEADER {
		nextStep, err = p.readPackHeader(pack, step)
	} else if step == PEER_READ_STEP_DATA {
		nextStep, err = p.readPackData(pack)
	} else {
		nextStep, err = PEER_READ_STEP_UNKNOWN, ErrPeerReadStepErr
	}

	return nextStep, err
}

func (p *Peer) readPackHeader(pack *Pack, step int) (int, error) {
	var err error = nil
	defer p.ec.DeferThrow("readPackHeader", &err)

	nextStep := step
	// data enough, handle direct
	if p.readBuff.GetDataLen() >= p.wantReadLen {
		nextStep, err = p.handleHeaderData(pack, step)
		return nextStep, err
	}

	if p.readBuff.GetDataLen()+p.readBuff.GetWriteBuffSize() < p.wantReadLen {
		p.readBuff.MoveDataToBegin()
	}

	n, err := p.readBytes(p.readBuff.GetWriteBuff())
	if err != nil || n == 0 {
		return step, err
	}

	p.readBuff.UpdateWriteOffset(uint32(n))
	if p.readBuff.GetDataLen() < p.wantReadLen {
		return step, nil
	}

	nextStep, err = p.handleHeaderData(pack, step)
	return nextStep, err
}

func (p *Peer) handleHeaderData(pack *Pack, step int) (int, error) {
	var err error = nil
	defer p.ec.DeferThrow("handleHeaderData", &err)

	data := p.readBuff.GetData()

	if step == PEER_READ_STEP_MARK { // mark
		err := pack.Header.UnmarshalMark(data[:p.wantReadLen])
		if err != nil {
			return step, err
		}

		// verify first
		if !pack.Header.VerifyMark() {
			err = ErrPeerMarkErr
			return step, err
		}

		headerLen := pack.Header.GetHeaderLen()
		// if headerLen == p.wantReadLen { // data enough
		// 	return p.handleHeaderData(pack, PEER_READ_STEP_HEADER)
		// }

		// need read more data
		p.wantReadLen = headerLen
		return PEER_READ_STEP_HEADER, nil

	} else if step == PEER_READ_STEP_HEADER { // header
		pack.Header.Unmarshal(data[:p.wantReadLen])
		p.readBuff.Skip(uint32(p.wantReadLen))

		payloadLen := pack.Header.GetPayloadLen()
		if payloadLen > p.maxPayload {
			err = ErrPeerReadPackTooBig
			return step, err
		}

		// if p.bBindSrc {
		// 	pack.Header.SrcPeerType = p.peerType
		// 	pack.Header.SrcPeerNo = p.peerNo
		// }

		if payloadLen == 0 {
			return PEER_READ_STEP_END, nil
		}

		p.wantReadLen = payloadLen
		return PEER_READ_STEP_DATA, nil
	}

	return PEER_READ_STEP_UNKNOWN, ErrPeerReadStepErr
}

func (p *Peer) readPackData(pack *Pack) (int, error) {
	var err error = nil
	defer p.ec.DeferThrow("readPackData", &err)

	payloadLen := pack.Header.GetPayloadLen()
	if payloadLen == 0 {
		return PEER_READ_STEP_END, nil
	}

	if p.isSingleFramePack(pack) { // single frame
		err = p.readPackFrame(pack, payloadLen)
		if err != nil {
			return PEER_READ_STEP_DATA, err
		}

		return PEER_READ_STEP_END, nil

	} else { // multi frames
		err = p.readPackFrames(pack, payloadLen)
		if err != nil {
			return PEER_READ_STEP_DATA, err
		}

		return PEER_READ_STEP_END, nil
	}
}

func (p *Peer) readPackFrames(pack *Pack, payloadLen int) error {
	var err error = nil
	defer p.ec.DeferThrow("readPackFrames", &err)

	leftLen := payloadLen
	frameSize := int(0)

	for {
		frameSize = leftLen
		if frameSize > PACK_MAX_FRAME {
			frameSize = PACK_MAX_FRAME
		}

		err = p.readPackFrame(pack, frameSize)
		if err != nil {
			break
		}

		leftLen -= frameSize
		if leftLen == 0 {
			break
		}
	}

	return err
}

func (p *Peer) readPackFrame(pack *Pack, payloadLen int) error {
	buffRef := p.buffFactory.CreateBuff(uint32(payloadLen))
	oriBuff := *buffRef
	payloadBuff := oriBuff[:payloadLen]
	err := p.readPackFrameImpl(payloadBuff)
	if err != nil {
		p.buffFactory.ReuseBuff(buffRef)
		return p.ec.Throw("readPackFrame", err)
	}

	pack.AddReuseFrame(oriBuff, uint(payloadLen))
	return nil
}

func (p *Peer) readPackFrameImpl(frameBuff []byte) error {
	var err error = nil
	defer p.ec.DeferThrow("readPackFrame", &err)

	frameSize := len(frameBuff)
	totalSize := int(0)
	readSize := int(0)
	// frameBuff := make(PackFrame, frameSize)

	// read from buffer first
	if p.readBuff.GetDataLen() > 0 {
		readSize, err = p.readBuff.Read(frameBuff)
		if err != nil {
			return err
		}

		if readSize == frameSize {
			return nil
		}

		totalSize += readSize
	}

	// read from conn
	for {
		readSize, err = p.readBytes(frameBuff[totalSize:])
		if err != nil {
			break
		}

		if readSize > 0 {
			totalSize += readSize
			if totalSize == frameSize {
				break
			}
		}
	}

	return err
}

func (p *Peer) isSingleFramePack(pack *Pack) bool {
	if !pack.Header.HasDst() {
		return true
	}

	peerType, peerNo := pack.Header.GetDstPeer()
	if peerType == p.mgr.GetOwnerType() && peerNo == p.mgr.GetOwnerNo() {
		return true
	}

	if pack.Header.GetPayloadLen() <= PACK_MAX_FRAME {
		return true
	}

	return false
}

func (p *Peer) readBytes(buff []byte) (int, error) {
	// timeNow := time.Now().Unix()
	// if p.isBadPeer(timeNow) {
	// 	p.ForceClose()
	// 	return 0, ErrPeerBad
	// }

	var err error = nil
	defer p.ec.DeferThrow("readBytes", &err)

	stat := p.getStat()
	if stat != PEER_STAT_OPEN {
		if stat == PEER_STAT_CLOSE_READ {
			err = ErrPeerCloseRead
		} else {
			err = ErrPeerClose
		}

		return 0, err
	}

	err = p.conn.SetReadDeadline(time.Now().Add(PEER_READ_TIMEOUT))
	if err != nil {
		return 0, err
	}

	n, err := p.conn.Read(buff)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, nil
		}

		return 0, err
	}

	return n, nil
}

func (p *Peer) writePack(pack *Pack) error {
	var err error = nil
	defer p.ec.DeferThrow("writePack", &err)

	header, err := pack.Header.Marshal()
	if err != nil {
		return err
	}

	if p.bSingleBuffPack {
		var buff []byte = nil
		buff, err = p.toOneBuffer(header, pack.Payload)
		if err != nil {
			return err
		}

		err = p.writeBytes(buff)
		return err
	}

	err = p.writeBytes(header)
	if err != nil {
		return err
	}

	// payload
	if pack.Header.GetPayloadLen() == 0 {
		return nil
	}

	for _, frame := range pack.Payload {
		err = p.writeBytes(frame)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Peer) toOneBuffer(header []byte, frames []PackFrame) ([]byte, error) {
	if len(frames) == 0 {
		if len(header) == 0 {
			return nil, ErrPeerEmptyPack
		}

		return header, nil
	}

	headerLen := len(header)
	dataLen := headerLen
	for _, frame := range frames {
		dataLen += len(frame)
	}

	if dataLen == 0 {
		return nil, ErrPeerEmptyPack
	}

	buff := make([]byte, dataLen)
	offset := 0
	if headerLen > 0 {
		copy(buff, header)
		offset += headerLen
	}

	for _, frame := range frames {
		frameLen := len(frame)
		if frameLen > 0 {
			copy(buff[offset:], frame)
			offset += frameLen
		}
	}

	return buff, nil
}

func (p *Peer) writeBytes(b []byte) error {
	var err error = nil
	defer p.ec.DeferThrow("writeBytes", &err)

	buffLen := len(b)
	totalWriteLen := 0

	if buffLen == 0 {
		return nil
	}

	for {
		err = p.conn.SetWriteDeadline(time.Now().Add(PEER_WRITE_TIMEOUT))
		if err != nil {
			break
		}

		n, err := p.conn.Write(b[totalWriteLen:])
		if err != nil {
			break
		}

		totalWriteLen += n
		if totalWriteLen >= buffLen {
			break
		}
	}

	return err
}

func (p *Peer) recordHealthyInfo(pack *Pack) {
	if !pack.Header.IsPongPack() {
		p.actTime = time.Now().UnixNano()
	}

	if pack.Header.IsDataPack() {
		p.bHasDataOpt = true
	}

	if !p.bStatSendInfo {
		return
	}

	// send info
	p.lckSendInfo.Lock()
	defer p.lckSendInfo.Unlock()

	p.sendCnt++

	packLen := pack.Header.GetHeaderLen() + pack.Header.GetPayloadLen()
	p.sendSize += packLen
}

func (p *Peer) isBadPeer(cfg *HealtyCfg, startTimeNanosec int64, nowNanosec int64) bool {
	if nowNanosec-p.connStartTime >= cfg.MaxIdleTime*int64(time.Second) {
		if !p.bHasDataOpt {
			return true
		}
	}

	if nowNanosec-p.actTime > cfg.MaxActIntv*int64(time.Second) {
		return true
	}

	// send info
	p.lckSendInfo.Lock()
	defer p.lckSendInfo.Unlock()

	actDuration := (nowNanosec - startTimeNanosec) / int64(time.Millisecond)
	if (cfg.MaxSendFreq != 0) && (p.sendCnt*1000 > cfg.MaxSendFreq*int(actDuration)) {
		return true
	}

	if (cfg.MaxSendUnit != 0) && (p.sendSize*1000 > cfg.MaxSendUnit*int(actDuration)) {
		return true
	}

	p.sendCnt = 0
	p.sendSize = 0
	return false
}

func (p *Peer) setStat(stat uint8) {
	p.lckStat.Lock()
	defer p.lckStat.Unlock()

	if p.stat < stat {
		p.stat = stat
	}
}

func (p *Peer) getStat() uint8 {
	p.lckStat.RLock()
	defer p.lckStat.RUnlock()

	return p.stat
}

func (p *Peer) notifyError(err error) {
	if p.needNotifyError() {
		p.mgr.OnPeerError(p, err)
	}
}

func (p *Peer) needNotifyError() bool {
	p.lckStat.Lock()
	defer p.lckStat.Unlock()

	if p.stat >= PEER_STAT_ERROR {
		return false
	}

	p.stat = PEER_STAT_ERROR
	return true
}

func (p *Peer) closeConnInner() {
	p.lckStat.Lock()
	defer p.lckStat.Unlock()

	p.closeConn()
}

func (p *Peer) closeWrite() {
	if p.stat >= PEER_STAT_CLOSE_WRITE {
		return
	}

	p.stat = PEER_STAT_CLOSE_WRITE

	close(p.queReadPacks)

	// wake up write loop and note to exit
	close(p.chanCloseWrite)
	// close(p.queWritePacks)
}

func (p *Peer) closeConn() {
	if p.stat >= PEER_STAT_CLOSE_CONN {
		return
	}

	p.stat = PEER_STAT_CLOSE_CONN
	p.conn.Close()
}
