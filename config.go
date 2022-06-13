// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

type BaseConfig struct {
	IpConnCntLimit uint32 `json:"ip_conn_limit"`
	MaxReadQue     uint32 `json:"max_read_queue"`
	MaxWriteQue    uint32 `json:"max_write_queue"`
	Network        string `json:"network"`
	Address        string `json:"address"`
	Port           uint16 `json:"port"`
	BindPeerType   uint32 `json:"bind_peer_type"`
	MinPeerNo      uint64 `json:"min_peer_no"`
	MaxPeerNo      uint64 `json:"max_peer_no"`
	MaxIdleTime    int64  `json:"max_idle_time_second"`
	MaxActIntv     int64  `json:"max_act_intv_second"`
	MaxSendFreq    int    `json:"max_send_cnt_per_second"`
	MaxSendUnit    int    `json:"max_send_size_per_second"`
}

type SocketConfig struct {
	BaseConfig
}

type WebSockConfig struct {
	BaseConfig
	Pattern       string `json:"pattern"`
	ReadBuffSize  int    `json:"read_buff_size"`
	WriteBuffSize int    `json:"write_buff_size"`
	CheckOrigin   bool   `json:"check_origin"`
}
