package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/danielhookx/eventbus"
	"github.com/nareix/joy5/av"
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

func (s *stream) addSub(closeCh <-chan bool, w av.PacketWriter, bus eventbus.Eventbus, ctx context.Context) error {
	ss := &streamSub{
		notify: make(chan struct{}, 1),
	}

	s.sub.Store(ss, nil)
	defer s.sub.Delete(ss)

	var cursor *gopCacheReadCursor
	var lastsp *streamPub

	seqsplit := splitSeqhdr{
		cb: func(pkt av.Packet) error {

			// for debug purposes
			if pkt.Type == av.Metadata {
				log.Println("pkt> Metadata>", pkt.String())
			} else if pkt.Type == av.H264DecoderConfig {
				log.Println("pkt>", av.PacketTypeString[pkt.Type], pkt.Time)
			} else if pkt.Type == av.AACDecoderConfig {
				log.Println("pkt>", av.PacketTypeString[pkt.Type], pkt.Data, pkt.Time)
			} else {
				// log.Printf("%-4v %-14v", av.PacketTypeString[pkt.Type], pkt.Time)
			}

			return w.WritePacket(pkt)
		},
	}

	busHnd := func(cmd string) {
		if cmd == "reset" {
			log.Println("sub busHnd >", cmd)
			seqsplit.reset()
		}
	}
	bus.Subscribe("/sub", busHnd)
	defer func() {
		bus.Unsubscribe("/sub", busHnd)
	}()

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
			case <-ctx.Done():
				return nil
			case <-closeCh:
				return errors.New("sub close")
			case <-ss.notify:
			}
		} else {
			for _, pkt := range pkts {
				if err := seqsplit.do(pkt); err != nil {
					log.Println("sub seqsplit.do!", err)
					return err
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
