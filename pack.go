// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"errors"
	"sync"

	"github.com/yxlib/yx"
)

var (
	ErrPackFrameIsNil     = errors.New("frame is nil")
	ErrPackFrameSizeWrong = errors.New("frame size is wrong")
)

const (
	PACK_MAX_FRAME   = (4 * 1024)
	PACK_MAX_PAYLOAD = (4 * 1024 * 8)
)

//========================
//        PackHeader
//========================
type PackHeaderSerialize interface {
	GetMarkLen() int
	UnmarshalMark([]byte) error

	GetHeaderLen() int
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
}

type PackHeaderOpr interface {
	VerifyMark() bool

	SetBinDataPack()
	SetTextDataPack()
	IsDataPack() bool

	SetPingPack()
	IsPingPack() bool

	SetPongPack()
	IsPongPack() bool

	SetRegisterReqPack()
	IsRegisterReqPack() bool

	SetRegisterRespPack()
	IsRegisterRespPack() bool

	SetHasSrc(bHasSrc bool)
	HasSrc() bool
	SetSrcPeer(peerType uint32, peerNo uint32)
	GetSrcPeer() (peerType uint32, peerNo uint32)

	SetHasDst(bHasDst bool)
	HasDst() bool
	SetDstPeer(peerType uint32, peerNo uint32)
	GetDstPeer() (peerType uint32, peerNo uint32)

	GetPayloadLen() int
	SetPayloadLen(payloadLen int)
}

type PackHeader interface {
	PackHeaderSerialize
	PackHeaderOpr
	Reset()
}

type PackHeaderFactory interface {
	CreateHeader() PackHeader
}

//========================
//         Pack
//========================
type PackFrame = []byte

type Pack struct {
	Header   PackHeader
	Payload  []PackFrame
	oriBuffs []PackFrame
	ec       *yx.ErrCatcher
}

func NewPack(h PackHeader) *Pack {
	return &Pack{
		Header:   h,
		Payload:  make([]PackFrame, 0),
		oriBuffs: make([]PackFrame, 0),
		ec:       yx.NewErrCatcher("p2pnet.Pack"),
	}
}

func NewSingleFramePack(h PackHeader, payload []byte) *Pack {
	p := NewPack(h)
	p.AddFrame(payload)
	p.UpdatePayloadLen()
	return p
}

// func (p *Pack) GetHeader() PackHeader {
// 	return p.Header
// }

func (p *Pack) AddFrame(frame []byte) error {
	if len(frame) == 0 {
		return p.ec.Throw("AddFrame", ErrPackFrameIsNil)
	}

	p.Payload = append(p.Payload, frame)
	return nil
}

func (p *Pack) AddReuseFrame(oriBuff []byte, frameSize uint) error {
	if len(oriBuff) == 0 || frameSize == 0 {
		return p.ec.Throw("AddReuseFrame", ErrPackFrameIsNil)
	}

	if len(oriBuff) < int(frameSize) {
		return p.ec.Throw("AddReuseFrame", ErrPackFrameSizeWrong)
	}

	p.oriBuffs = append(p.oriBuffs, oriBuff)
	p.AddFrame(oriBuff[:frameSize])
	return nil
}

func (p *Pack) AddFrames(frames []PackFrame) error {
	for _, frame := range frames {
		err := p.AddFrame(frame)
		if err != nil {
			return p.ec.Throw("AddFrames", err)
		}
	}

	return nil
}

func (p *Pack) UpdatePayloadLen() {
	payloadLen := 0
	for _, frame := range p.Payload {
		payloadLen += len(frame)
	}

	p.Header.SetPayloadLen(payloadLen)
}

func (p *Pack) Reset() {
	p.Header.Reset()
	p.Payload = make([]PackFrame, 0)
	p.oriBuffs = make([]PackFrame, 0)
}

//========================
//         PackPool
//========================
type PackPool struct {
	pool          *sync.Pool
	headerFactory PackHeaderFactory
}

func NewPackPool(headerFactory PackHeaderFactory) *PackPool {
	p := &PackPool{
		pool:          &sync.Pool{},
		headerFactory: headerFactory,
	}

	p.pool.New = p.newPack

	return p
}

func (p *PackPool) Put(pack *Pack) {
	pack.Reset()
	p.pool.Put(pack)
}

func (p *PackPool) Get() *Pack {
	obj := p.pool.Get()
	return obj.(*Pack)
}

func (p *PackPool) newPack() interface{} {
	h := p.headerFactory.CreateHeader()
	return NewPack(h)
}
