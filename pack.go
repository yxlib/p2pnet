// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"errors"

	"github.com/yxlib/yx"
)

var (
	ErrPackFrameIsNil = errors.New("frame is nil")
)

const (
	PACK_MAX_FRAME   = (4 * 1024)
	PACK_MAX_PAYLOAD = (4 * 1024 * 8)
)

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
}

type PackHeader interface {
	PackHeaderSerialize
	PackHeaderOpr
}

type PackHeaderFactory interface {
	CreateHeader() PackHeader
}

type PackFrame = []byte

type Pack struct {
	Header  PackHeader
	Payload []PackFrame
	ec      *yx.ErrCatcher
}

func NewPack(h PackHeader) *Pack {
	return &Pack{
		Header:  h,
		Payload: make([]PackFrame, 0),
		ec:      yx.NewErrCatcher("p2pnet.Pack"),
	}
}

func NewSingleFramePack(h PackHeader, payload []byte) *Pack {
	p := NewPack(h)
	p.AddFrame(payload)

	return p
}

// func (p *Pack) GetHeader() PackHeader {
// 	return p.Header
// }

func (p *Pack) AddFrame(frame []byte) error {
	if nil == frame {
		return p.ec.Throw("AddFrame", ErrPackFrameIsNil)
	}

	p.Payload = append(p.Payload, frame)
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
