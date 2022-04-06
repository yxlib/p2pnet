// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"errors"
	"net"
	"os"
	"strings"
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
	ErrPeerClose            = errors.New("peer close")
)

const (
	PEER_READ_TIMEOUT   = time.Second * 3
	PEER_WRITE_TIMEOUT  = time.Second * 3
	PEER_READ_BUFF_SIZE = 4 * 1024
)

const (
	PEER_READ_STEP_UNKNOWN = iota
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
	maxIdleTime int64
	maxActIntv  int64
	maxSendFreq int
	maxSendUnit int
}

func GetRemoteAddr(c net.Conn) string {
	ipAddr := c.RemoteAddr().String()
	subStrs := strings.Split(ipAddr, ":")
	return subStrs[0]
}

type PeerListener interface {
	GetOwnerType() uint32
	GetOwnerNo() uint32
	OnPeerError(p *Peer, err error)
	OnPeerClose(p *Peer)
	OnPeerRead(p *Peer)
}

type Peer struct {
	peerType    uint32
	peerNo      uint32
	conn        net.Conn
	ipAddr      string
	bCloseRead  bool
	bExit       bool
	bForceClose bool
	lckClose    *sync.Mutex

	mgr           PeerListener
	headerFactory PackHeaderFactory

	wantReadLen   int
	readBuff      *yx.SimpleBuffer
	evtExitRead   *yx.Event
	queReadPacks  chan *Pack
	queWritePacks chan *Pack

	connStartTime int64
	bHasDataOpt   bool
	actTime       int64
	bStatSendInfo bool
	lckSendInfo   *sync.Mutex
	sendCnt       int
	sendSize      int

	ec *yx.ErrCatcher
}

func NewPeer(peerType uint32, peerNo uint32, c net.Conn, maxReadQue uint32, maxWriteQue uint32) *Peer {
	return &Peer{
		peerType:      peerType,
		peerNo:        peerNo,
		conn:          c,
		ipAddr:        GetRemoteAddr(c),
		bCloseRead:    false,
		bExit:         false,
		bForceClose:   false,
		lckClose:      &sync.Mutex{},
		mgr:           nil,
		headerFactory: nil,
		wantReadLen:   0,
		readBuff:      yx.NewSimpleBuffer(PEER_READ_BUFF_SIZE),
		evtExitRead:   yx.NewEvent(),
		queReadPacks:  make(chan *Pack, maxReadQue),
		queWritePacks: make(chan *Pack, maxWriteQue),
		connStartTime: 0,
		bHasDataOpt:   false,
		actTime:       0,
		bStatSendInfo: false,
		lckSendInfo:   &sync.Mutex{},
		sendCnt:       0,
		sendSize:      0,
		ec:            yx.NewErrCatcher("p2pnet.Peer"),
	}
}

func (p *Peer) SetMgr(mgr PeerListener) {
	p.mgr = mgr
}

func (p *Peer) SetHeaderFactory(headerFactory PackHeaderFactory) {
	p.headerFactory = headerFactory
}

func (p *Peer) GetHeaderFactory() PackHeaderFactory {
	return p.headerFactory
}

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

func (p *Peer) Open() {
	p.connStartTime = time.Now().Unix()

	go p.readLoop()
	go p.writeLoop()
}

func (p *Peer) CloseRead() {
	p.bCloseRead = true
}

func (p *Peer) Close() {
	p.lckClose.Lock()
	defer p.lckClose.Unlock()

	if p.bForceClose {
		return
	}

	p.bForceClose = true
	p.bCloseRead = true
	p.conn.Close()
}

func (p *Peer) IsExit() bool {
	return p.bExit
}

func (p *Peer) PopReadPack() (*Pack, error) {
	if len(p.queReadPacks) == 0 {
		return nil, ErrPeerReadQueEmpty
	}

	pack, ok := <-p.queReadPacks
	if !ok {
		return nil, ErrPeerReadQueClose
	}

	return pack, nil
}

func (p *Peer) PushWritePack(pack *Pack) error {
	if nil == pack {
		return p.ec.Throw("PushWritePack", ErrPeerWritePackNil)
	}

	p.queWritePacks <- pack
	return nil
}

func (p *Peer) SetStatSendInfo() {
	p.bStatSendInfo = true
}

func (p *Peer) CheckHealthy(cfg *HealtyCfg, startTime int64, now int64) {
	if p.isBadPeer(cfg, startTime, now) {
		p.Close()
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

	close(p.queReadPacks)
	p.mgr.OnPeerError(p, err)

	// wake up write loop and note to exit
	close(p.queWritePacks)

	p.evtExitRead.Send()
}

func (p *Peer) writeLoop() {
	var err error = nil

	for {
		pack, ok := <-p.queWritePacks
		if !ok {
			break
		}

		err = p.writePack(pack)
		if err != nil {
			break
		}
	}

	if err != nil {
		p.mgr.OnPeerError(p, err)
	}

	// wake up read loop and note to exit
	p.Close()
	p.evtExitRead.Wait()

	p.mgr.OnPeerClose(p)
	p.bExit = true
}

func (p *Peer) readPack() (*Pack, error) {
	var err error = nil
	header := p.headerFactory.CreateHeader()
	pack := NewPack(header)
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
		return nil, err
	}

	return pack, nil
}

func (p *Peer) readPackPart(pack *Pack, step int) (int, error) {
	if step == PEER_READ_STEP_MARK || step == PEER_READ_STEP_HEADER {
		return p.readPackHeader(pack, step)
	} else if step == PEER_READ_STEP_DATA {
		return p.readPackData(pack)
	} else {
		return PEER_READ_STEP_UNKNOWN, ErrPeerReadStepErr
	}
}

func (p *Peer) readPackHeader(pack *Pack, step int) (int, error) {
	// data enough, handle direct
	if p.readBuff.GetDataLen() >= p.wantReadLen {
		return p.handleHeaderData(pack, step)
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

	return p.handleHeaderData(pack, step)
}

func (p *Peer) handleHeaderData(pack *Pack, step int) (int, error) {
	data := p.readBuff.GetData()

	if step == PEER_READ_STEP_MARK { // mark
		err := pack.Header.UnmarshalMark(data[:p.wantReadLen])
		if err != nil {
			return step, err
		}

		// verify first
		if !pack.Header.VerifyMark() {
			return step, ErrPeerMarkErr
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
		payloadLen := pack.Header.GetPayloadLen()
		if payloadLen > PACK_MAX_PAYLOAD {
			return step, ErrPeerReadPackTooBig
		}

		// if p.bBindSrc {
		// 	pack.Header.SrcPeerType = p.peerType
		// 	pack.Header.SrcPeerNo = p.peerNo
		// }

		if payloadLen == 0 {
			return PEER_READ_STEP_END, nil
		}

		p.readBuff.Skip(uint32(p.wantReadLen))
		p.wantReadLen = payloadLen
		return PEER_READ_STEP_DATA, nil
	}

	return PEER_READ_STEP_UNKNOWN, ErrPeerReadStepErr
}

func (p *Peer) readPackData(pack *Pack) (int, error) {
	payloadLen := pack.Header.GetPayloadLen()
	if payloadLen == 0 {
		return PEER_READ_STEP_END, nil
	}

	if p.isSingleFramePack(pack) { // single frame
		payloadBuff, err := p.readPackFrame(payloadLen)
		if err != nil {
			return PEER_READ_STEP_DATA, err
		}

		pack.AddFrame(payloadBuff)
		return PEER_READ_STEP_END, nil

	} else { // multi frames
		frames, err := p.readPackFrames(payloadLen)
		if err != nil {
			return PEER_READ_STEP_DATA, err
		}

		pack.AddFrames(frames)
		return PEER_READ_STEP_END, nil
	}
}

func (p *Peer) readPackFrames(payloadLen int) ([]PackFrame, error) {
	var err error = nil
	var frames []PackFrame = make([]PackFrame, 0)
	var f PackFrame = nil
	leftLen := payloadLen
	frameSize := int(0)

	for {
		frameSize = leftLen
		if frameSize > PACK_MAX_FRAME {
			frameSize = PACK_MAX_FRAME
		}

		f, err = p.readPackFrame(frameSize)
		if err != nil {
			break
		}

		frames = append(frames, f)
		leftLen -= frameSize
		if leftLen == 0 {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	return frames, nil
}

func (p *Peer) readPackFrame(frameSize int) (PackFrame, error) {
	var err error = nil
	totalSize := int(0)
	readSize := int(0)
	frameBuff := make(PackFrame, frameSize)

	// read from buffer first
	if p.readBuff.GetDataLen() > 0 {
		readSize, err = p.readBuff.Read(frameBuff)
		if err != nil {
			return nil, err
		}

		if readSize == frameSize {
			return frameBuff, nil
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

	if err != nil {
		return nil, err
	}

	return frameBuff, nil
}

func (p *Peer) isSingleFramePack(pack *Pack) bool {
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

	if p.bCloseRead {
		return 0, ErrPeerClose
	}

	err := p.conn.SetReadDeadline(time.Now().Add(PEER_READ_TIMEOUT))
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
	buff, err := pack.Header.Marshal()
	if err != nil {
		return err
	}

	err = p.writeBytes(buff)
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

func (p *Peer) writeBytes(b []byte) error {
	var err error = nil
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
		p.actTime = time.Now().Unix()
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

func (p *Peer) isBadPeer(cfg *HealtyCfg, startTime int64, now int64) bool {
	if now-p.connStartTime >= cfg.maxIdleTime {
		if !p.bHasDataOpt {
			return true
		}
	}

	if now-p.actTime > cfg.maxActIntv {
		return true
	}

	// send info
	p.lckSendInfo.Lock()
	defer p.lckSendInfo.Unlock()

	actDuration := now - startTime
	if (cfg.maxSendFreq != 0) && (p.sendCnt > cfg.maxSendFreq*int(actDuration)) {
		return true
	}

	if (cfg.maxSendUnit != 0) && (p.sendSize > cfg.maxSendUnit*int(actDuration)) {
		return true
	}

	p.sendCnt = 0
	p.sendSize = 0
	return false
}
