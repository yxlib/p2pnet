package p2pnet

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
