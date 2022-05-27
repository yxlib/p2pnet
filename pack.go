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

//========================
//       PackHeader
//========================
type BasePackHeader struct {
}

func NewBasePackHeader() *BasePackHeader {
	return &BasePackHeader{}
}

func (h *BasePackHeader) Reset() {
}

func (h *BasePackHeader) GetMarkLen() int {
	return 0
}

func (h *BasePackHeader) UnmarshalMark(b []byte) error {
	return nil
}

func (h *BasePackHeader) GetHeaderLen() int {
	return 0
}

func (h *BasePackHeader) Unmarshal(buff []byte) error {
	return nil
}

func (h *BasePackHeader) Marshal() ([]byte, error) {
	return nil, nil
}

func (h *BasePackHeader) VerifyMark() bool {
	return true
}

func (h *BasePackHeader) SetBinDataPack() {
}

func (h *BasePackHeader) SetTextDataPack() {
}

func (h *BasePackHeader) IsDataPack() bool {
	return true
}

func (h *BasePackHeader) SetPingPack() {
}

func (h *BasePackHeader) IsPingPack() bool {
	return false
}

func (h *BasePackHeader) SetPongPack() {
}

func (h *BasePackHeader) IsPongPack() bool {
	return false
}

func (h *BasePackHeader) SetRegisterReqPack() {
}

func (h *BasePackHeader) IsRegisterReqPack() bool {
	return false
}

func (h *BasePackHeader) SetRegisterRespPack() {

}

func (h *BasePackHeader) IsRegisterRespPack() bool {
	return false
}

func (h *BasePackHeader) SetHasSrc(bHasSrc bool) {
}

func (h *BasePackHeader) HasSrc() bool {
	return false
}

func (h *BasePackHeader) SetSrcPeer(peerType uint32, peerNo uint32) {
}

func (h *BasePackHeader) GetSrcPeer() (peerType uint32, peerNo uint32) {
	return 0, 0
}

func (h *BasePackHeader) SetHasDst(bHasDst bool) {
}

func (h *BasePackHeader) HasDst() bool {
	return false
}

func (h *BasePackHeader) SetDstPeer(peerType uint32, peerNo uint32) {
}

func (h *BasePackHeader) GetDstPeer() (peerType uint32, peerNo uint32) {
	return 0, 0
}

func (h *BasePackHeader) SetHasPayloadLen(bHasPayloadLen bool) {
}

func (h *BasePackHeader) HasPayloadLen() bool {
	return false
}

func (h *BasePackHeader) GetPayloadLen() int {
	return 0
}

func (h *BasePackHeader) SetPayloadLen(payloadLen int) {
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
	if len(oriBuff) == int(frameSize) {
		p.AddFrame(oriBuff)
	} else {
		p.AddFrame(oriBuff[:frameSize])
	}

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
	if p.Header != nil {
		p.Header.Reset()
	}

	p.Payload = make([]PackFrame, 0)
	p.oriBuffs = make([]PackFrame, 0)
}

//========================
//         PackPool
//========================
type PackPool struct {
	pool            *sync.Pool
	headerFactory   PackHeaderFactory
	headerClassName string
}

func NewPackPool(headerFactory PackHeaderFactory) *PackPool {
	p := &PackPool{
		pool:            &sync.Pool{},
		headerFactory:   headerFactory,
		headerClassName: "",
	}

	p.pool.New = p.newPack

	if p.headerFactory != nil {
		tmpHeader := p.headerFactory.CreateHeader()
		p.headerClassName, _ = yx.GetClassReflectName(tmpHeader)
	}

	return p
}

func (p *PackPool) Put(pack *Pack) {
	if pack == nil {
		return
	}

	pack.Reset()
	if pack.Header != nil {
		name, _ := yx.GetClassReflectName(pack.Header)
		if name != p.headerClassName {
			pack.Header = nil
		}
	}

	p.pool.Put(pack)
}

func (p *PackPool) Get() *Pack {
	obj := p.pool.Get()
	pack := obj.(*Pack)
	if pack.Header == nil && p.headerFactory != nil {
		pack.Header = p.headerFactory.CreateHeader()
	}

	return pack
}

func (p *PackPool) newPack() interface{} {
	h := p.headerFactory.CreateHeader()
	return NewPack(h)
}
