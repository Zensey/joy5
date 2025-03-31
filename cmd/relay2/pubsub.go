package main

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nareix/joy5/av"
	"github.com/nareix/joy5/format/rtmp"
)

type streamSub struct {
	notify chan struct{}
}

type streamPub struct {
	cancel func()
	gc     *gopCache
}

type stream struct {
	n   int64
	sub sync.Map
	pub unsafe.Pointer
}

func (s *stream) curGopCacheSnapshot() *gopCacheSnapshot {
	sp := (*streamPub)(atomic.LoadPointer(&s.pub))
	if sp == nil {
		return nil
	}
	return sp.gc.curSnapshot()
}

func (s *stream) addSub(close <-chan bool, w av.PacketWriter) {
	ss := &streamSub{
		notify: make(chan struct{}, 1),
	}

	s.sub.Store(ss, nil)
	defer s.sub.Delete(ss)

	var cursor *gopCacheReadCursor
	var lastsp *streamPub

	seqsplit := splitSeqhdr{
		cb: func(pkt av.Packet) error {
			
			if pkt.Type == av.Metadata {
				log.Println("META>", pkt.Data, pkt.String())
				// return nil
			}

			if pkt.Type == av.AACDecoderConfig || pkt.Type == av.H264DecoderConfig {
				log.Println("pkt", av.PacketTypeString[pkt.Type], pkt.Time)
			}
			if pkt.Type == av.AACDecoderConfig {
				log.Println("pkt", av.PacketTypeString[pkt.Type], pkt.Data)
			}

			// log.Printf("%-4v %-14v", av.PacketTypeString[pkt.Type], pkt.Time)
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

			sp.gc.subStarted = true
		}

		if len(pkts) == 0 {
			sp.gc.subIdle = true

			select {
			case <-close:
				log.Println("sub close!")
				return
			case <-ss.notify:
				sp.gc.subIdle = false
			}
		} else {
			sp.gc.subIdle = false

			for _, pkt := range pkts {
				if err := seqsplit.do(pkt); err != nil {
					log.Println("sub seqsplit.do!", err)
					return
				}
			}
		}
	}
}

func (s *stream) notifySub() {
	s.sub.Range(func(key, value interface{}) bool {
		ss := key.(*streamSub)
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

func doPubsubRtmp(listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	s := rtmp.NewServer()
	handleRtmpServerFlags(s)

	streams := newStreams()

	s.LogEvent = func(c *rtmp.Conn, nc net.Conn, e int) {
		es := rtmp.EventString[e]
		log.Println(nc.LocalAddr(), nc.RemoteAddr(), es)
	}

	s.HandleConn = func(c *rtmp.Conn, nc net.Conn) {
		stream, remove := streams.add(c.URL.Path)
		defer remove()

		if c.Publishing {
			stream.setPub(c)
		} else {
			stream.addSub(c.CloseNotify(), c)
		}
	}

	for {
		nc, err := lis.Accept()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		go s.HandleNetConn(nc)
	}
}
