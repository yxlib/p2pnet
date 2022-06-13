// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"net"
	"strings"
)

func GetRemoteAddr(c net.Conn) string {
	ipAddr := c.RemoteAddr().String()
	subStrs := strings.Split(ipAddr, ":")
	return subStrs[0]
}

func GetBoundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return
	}

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = strings.Split(localAddr.String(), ":")[0]
	return
}
