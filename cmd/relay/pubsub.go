package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nareix/joy5/av"
	"github.com/nareix/joy5/format"
	"github.com/nareix/joy5/format/rtmp"
)

type gopCacheSnapshot struct {
	pkts []av.Packet
	idx  int
}

type gopCache struct {
	pkts  []av.Packet
	idx   int
	curst unsafe.Pointer
}

func (gc *gopCache) put(pkt av.Packet) {
	if pkt.IsKeyFrame {
		gc.pkts = []av.Packet{}
	}
	gc.pkts = append(gc.pkts, pkt)
	gc.idx++
	st := &gopCacheSnapshot{
		pkts: gc.pkts,
		idx:  gc.idx,
	}
	atomic.StorePointer(&gc.curst, unsafe.Pointer(st))
}

func (gc *gopCache) curSnapshot() *gopCacheSnapshot {
	return (*gopCacheSnapshot)(atomic.LoadPointer(&gc.curst))
}

type gopCacheReadCursor struct {
	lastidx int
}

func (rc *gopCacheReadCursor) advance(cur *gopCacheSnapshot) []av.Packet {
	lastidx := rc.lastidx
	rc.lastidx = cur.idx
	if diff := cur.idx - lastidx; diff <= len(cur.pkts) {
		return cur.pkts[len(cur.pkts)-diff:]
	} else {
		return cur.pkts
	}
}

type mergeSeqhdr struct {
	cb     func(av.Packet)
	hdrpkt av.Packet
}

func (m *mergeSeqhdr) do(pkt av.Packet) {
	switch pkt.Type {
	case av.H264DecoderConfig:
		m.hdrpkt.VSeqHdr = append([]byte(nil), pkt.Data...)
	case av.H264:
		pkt.Metadata = m.hdrpkt.Metadata
		if pkt.IsKeyFrame {
			pkt.VSeqHdr = m.hdrpkt.VSeqHdr
		}
		m.cb(pkt)
	case av.AACDecoderConfig:
		m.hdrpkt.ASeqHdr = append([]byte(nil), pkt.Data...)
	case av.AAC:
		pkt.Metadata = m.hdrpkt.Metadata
		pkt.ASeqHdr = m.hdrpkt.ASeqHdr
		m.cb(pkt)
	case av.Metadata:
		m.hdrpkt.Metadata = pkt.Data
	}
}

type splitSeqhdr struct {
	cb     func(av.Packet) error
	hdrpkt av.Packet
}

func (s *splitSeqhdr) sendmeta(pkt av.Packet) error {
	if bytes.Compare(s.hdrpkt.Metadata, pkt.Metadata) != 0 {
		if err := s.cb(av.Packet{
			Type: av.Metadata,
			Data: pkt.Metadata,
		}); err != nil {
			return err
		}
		s.hdrpkt.Metadata = pkt.Metadata
	}
	return nil
}

func (s *splitSeqhdr) do(pkt av.Packet) error {
	switch pkt.Type {
	case av.H264:
		if err := s.sendmeta(pkt); err != nil {
			return err
		}
		if pkt.IsKeyFrame {
			if bytes.Compare(s.hdrpkt.VSeqHdr, pkt.VSeqHdr) != 0 {
				if err := s.cb(av.Packet{
					Type: av.H264DecoderConfig,
					Data: pkt.VSeqHdr,
				}); err != nil {
					return err
				}
				s.hdrpkt.VSeqHdr = pkt.VSeqHdr
			}
		}
		return s.cb(pkt)
	case av.AAC:
		if err := s.sendmeta(pkt); err != nil {
			return err
		}
		if bytes.Compare(s.hdrpkt.ASeqHdr, pkt.ASeqHdr) != 0 {
			if err := s.cb(av.Packet{
				Type: av.AACDecoderConfig,
				Data: pkt.ASeqHdr,
			}); err != nil {
				return err
			}
			s.hdrpkt.ASeqHdr = pkt.ASeqHdr
		}
		return s.cb(pkt)
	}
	return nil
}

type streamSub struct {
	notify chan struct{}
	stop   chan struct{} // deactivate the sub
}

type streamPub struct {
	cancel func()
	gc     *gopCache
}

type stream struct {
	n   int64          // number of subs + pub
	sub sync.Map       // subscribers
	pub unsafe.Pointer //
}

func (s *stream) curGopCacheSnapshot() *gopCacheSnapshot {
	sp := (*streamPub)(atomic.LoadPointer(&s.pub))
	if sp == nil {
		return nil
	}
	return sp.gc.curSnapshot()
}

func (s *stream) notifySub() {
	s.sub.Range(func(key, value interface{}) bool {
		ss := value.(*streamSub)
		select {
		case ss.notify <- struct{}{}:
		default:
		}
		return true
	})
}

func (s *stream) setPub(r av.PacketReader) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sp := &streamPub{
		cancel: cancel,
		gc:     &gopCache{},
	}

	oldsp := (*streamPub)(atomic.SwapPointer(&s.pub, unsafe.Pointer(sp)))
	if oldsp != nil {
		oldsp.cancel()
	}

	seqmerge := mergeSeqhdr{
		cb: func(pkt av.Packet) {
			sp.gc.put(pkt)
			s.notifySub()
		},
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pkt, err := r.ReadPacket()
		if err != nil {
			return
		}
		seqmerge.do(pkt)
	}
}

type streams struct {
	l sync.RWMutex
	m map[string]*stream
}

func newStreams() *streams {
	return &streams{
		m: map[string]*stream{},
	}
}

func (ss *streams) get(k string) *stream {
	ss.l.Lock()
	defer ss.l.Unlock()

	return ss.m[k]
}

func (ss *streams) add(k string) (*stream, func()) {
	ss.l.Lock()
	defer ss.l.Unlock()

	log.Println("stream", k, "add")

	s, ok := ss.m[k]
	if !ok {
		s = &stream{}
		ss.m[k] = s
	}
	s.n++

	return s, func() {
		log.Println("stream", k, "remove")

		ss.l.Lock()
		defer ss.l.Unlock()

		s.n--
		if s.n == 0 {
			delete(ss.m, k)
		}
	}
}

// add external sub, we are actively restreaming to
func (s *stream) addSubExt(ss *streamSub, key string, close <-chan bool, w av.PacketWriter) {

	var cursor *gopCacheReadCursor
	var lastsp *streamPub

	seqsplit := splitSeqhdr{
		cb: func(pkt av.Packet) error {
			return w.WritePacket(pkt)
		},
	}

	for {
		var pkts []av.Packet

		sp := (*streamPub)(atomic.LoadPointer(&s.pub))
		if sp != lastsp {
			cursor = &gopCacheReadCursor{}
			lastsp = sp
		}
		if sp != nil {
			cur := sp.gc.curSnapshot()
			if cur != nil {
				pkts = cursor.advance(cur)
			}
		}

		if len(pkts) == 0 {
			select {
			case <-ss.stop:
				log.Println("stop substream", key)
				return
			case <-close:
				return
			case <-ss.notify:
			}
		} else {
			for _, pkt := range pkts {
				if err := seqsplit.do(pkt); err != nil {
					return
				}
			}
		}
	}
}

func activateSubStream(s *stream, ep_id, ep_url string) {
	log.Println("activateSub", ep_id, ep_url)

	ss := &streamSub{
		notify: make(chan struct{}, 1),
		stop:   make(chan struct{}, 1),
	}
	s.sub.Store(ep_id, ss)
	defer s.sub.Delete(ep_id)

	fo := newFormatOpener()
	var err error
	var w *format.Writer
	for {
		select {
		case <-ss.stop:
			log.Println("stop substream creation key:", ep_id)
			return
		default:
		}

		if w, err = fo.Create(ep_url); err != nil {
			log.Println("DialFailed", err)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	nc2 := w.NetConn
	defer nc2.Close()
	log.Println("Dial outbound OK")

	log.Println("activate substream", ep_id, ep_url)
	s.addSubExt(ss, ep_id, w.Rtmp.CloseNotify(), w)
	log.Println("deactivate substream", ep_id, ep_url)
}

type pubsubService struct {
	streams *streams
	lis     net.Listener
}

func (s *pubsubService) StopSubStream(key, subkey string) {
	stream := s.streams.get(key)
	if stream == nil {
		log.Println("no active stream!")
		return
	}
	stream.sub.Range(func(key, value interface{}) bool {
		if key.(string) == subkey {
			p := value.(*streamSub)
			p.stop <- struct{}{}
		}
		return true
	})
}

func (s *pubsubService) handleRtmpConn(c *rtmp.Conn, nc net.Conn) {
	streamPublishPrefix := "/live/"

	if !strings.HasPrefix(c.URL.Path, streamPublishPrefix) {
		return
	}
	pubkey := strings.TrimPrefix(c.URL.Path, streamPublishPrefix)
	log.Println("[HandleConn] pubkey:", pubkey)

	stream, remove := s.streams.add(pubkey)
	defer remove()

	if c.Publishing {
		account := config.Accounts[pubkey]

		for epID, ep := range account.Endpoints {
			if !ep.Enabled {
				continue
			}
			// make local copy to avoid a race
			epID, epURL := epID, ep.URL
			go activateSubStream(stream, epID, epURL)
		}

		stream.setPub(c)
	}
}

func doPubsubRtmp(listenAddr string) (svc *pubsubService, err error) {
	svc = &pubsubService{}
	svc.lis, err = net.Listen("tcp", listenAddr)
	if err != nil {
		return
	}
	svc.streams = newStreams()

	s := rtmp.NewServer()

	handleRtmpServerFlags(s)
	s.LogEvent = func(c *rtmp.Conn, nc net.Conn, e int) {
		es := rtmp.EventString[e]
		log.Println(nc.LocalAddr(), nc.RemoteAddr(), es)
	}
	s.HandleConn = svc.handleRtmpConn

	go func() {
		for {
			nc, err := svc.lis.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}

				time.Sleep(time.Second)
				continue
			}
			go s.HandleNetConn(nc)
		}
	}()
	return
}

func (s *pubsubService) Stop() {
	s.lis.Close()
}
