// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package p2pnet

import (
	"net"
	"time"
)

type Client interface {
	DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error)
}
