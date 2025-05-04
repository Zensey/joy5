package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/danielhookx/eventbus"
	"github.com/nareix/joy5/av"
	"github.com/nareix/joy5/codec/aac"
	"github.com/nareix/joy5/format/flv"
	"github.com/nareix/joy5/format/flv/flvio"
)

func audioSource(ctx context.Context, seqmerge *mergeSeqhdr, t0 time.Time, url string, bus eventbus.Eventbus) {
	defer func() {
		log.Println("audioSource exit")
	}()

	adtsHdr := make([]byte, adtsHeaderSize)
	adtsHdr[0] = 0xff
	adtsHdr[1] = 0xf1

	pkt := av.Packet{}
	pkt.Type = av.AACDecoderConfig
	pkt.Time = 0
	pkt.Data = []byte{18, 16, 86, 229, 0}
	seqmerge.do(pkt)

	lag := time.Duration(0)
	offset := time.Duration(0)

	ctrl_ := make(chan string)
	dispatchCmd := func(cmd string) {
		ctrl_ <- cmd
	}
	bus.Subscribe("/src", dispatchCmd)
	defer bus.Unsubscribe("/src", dispatchCmd)

	connectAndCopy := func() error {
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("Error connecting to stream:", err)
			return err
		}
		defer resp.Body.Close()

		log.Println("audioSource> Dial OK")
		reader := bufio.NewReaderSize(resp.Body, 4096*4)

		skip := false
		for {
			select {
			case <-ctx.Done():
				log.Println("audioSource Done!")
				return nil

			case cmd := <-ctrl_:
				log.Println("audioSource cmd>", cmd)
				switch cmd {
				case "stop":
					skip = true
				case "start":
					t0 = time.Now()
					lag = time.Duration(0)
					offset = time.Duration(0)
					skip = false
				}
			default:
			}

			err := findADTSHeader(adtsHdr, reader)
			if err != nil {
				log.Println("No ADTS header found!")
				return err
			}

			config, hdrlen, framelen, _, err := aac.ParseADTSHeader(adtsHdr)
			if err != nil {
				log.Println("ADTS header parse error:", err)
				log.Println("ADTS:", adtsHdr)
				return err
			}
			if hdrlen != 7 {
				log.Println("Assert! ADTS header len!", hdrlen)
			}

			frame := make([]byte, framelen)
			copy(frame[0:7], adtsHdr)
			_, err = io.ReadFull(reader, frame[7:])
			if err != nil {
				log.Println("Read frame error:", err)
				return err
			}

			pkt := av.Packet{}
			pkt.Type = av.AAC
			pkt.Time = offset
			// pkt.Data = getEmptyAAC()
			// pkt.Data = frame[7:] // adts header is optional!
			pkt.Data = frame

			pktDuration := aac.PacketDuration(config, nil)
			delay := pktDuration - lag
			if delay > 0 {
				// log.Println("audio> delay", delay)
				time.Sleep(delay)
			}
			lag = time.Since(t0) - pkt.Time

			if !skip {
				seqmerge.do(pkt)
			}
			// log.Printf("%-4v %-16v %-14v %-14v", av.PacketTypeString[pkt.Type], pkt.Time, lag, pktDuration)
			offset += pktDuration
		}
	}

	for {
		connectAndCopy()
	}
}

func videoSource(ctx context.Context, seqmerge *mergeSeqhdr, vf *os.File, t0 time.Time, bus eventbus.Eventbus) {
	defer func() {
		log.Println("videoSource exit")
	}()
	log.Println("videoSource start")

	lag := time.Duration(0)
	offset := time.Duration(0)
	videoDuration := time.Duration(0)

	controll := make(chan string)
	dispatchCmd := func(cmd string) {
		controll <- cmd
	}
	bus.Subscribe("/src", dispatchCmd)
	defer bus.Unsubscribe("/src", dispatchCmd)

	for {
		vf.Seek(0, 0)
		r := flv.NewDemuxer(vf)
		pktTime := time.Duration(0)
		pktTimePrev := time.Duration(0)

		for {
			select {
			case <-ctx.Done():
				log.Println("videoSource Done!")
				return

			case cmd := <-controll:
				log.Println("videoSource > cmd", cmd)
				if cmd == "stop" {
					return
				}
			default:
			}

			pkt, err := r.ReadPacket()
			if err != nil {
				if err != io.EOF {
					log.Println("ReadPacket err:", err)
				}
				break
			}

			if pkt.Type == av.Metadata {
				amf, _ := flvio.ParseAMFVals(pkt.Data, false)
				m := amf[0].(flvio.AMFMap)
				duration, _ := m.GetFloat64("duration")
				videoDuration = durationFromFloat64(duration)
				continue
			}
			// ignore audio track
			if pkt.Type == av.AAC || pkt.Type == av.AACDecoderConfig {
				continue
			}

			delay := pkt.Time - pktTimePrev - lag
			pktTimePrev = pkt.Time
			pkt.Time += offset
			pktTime = pkt.Time

			if delay > 0 {
				time.Sleep(delay)
			}
			seqmerge.do(pkt)

			lag = time.Since(t0) - pktTime
			// log.Printf("%-4v %-16v %-14v", av.PacketTypeString[pkt.Type], pkt.Time, lag)
		}

		offset += videoDuration
		rest := videoDuration - pktTimePrev - lag
		time.Sleep(rest)
		lag = time.Since(t0) - offset
	}
}

func (s *stream) setPubFromFile(videoFile, accStreamUrl string, bus eventbus.Eventbus) {
	defer func() {
		log.Println("setPubFromFile exit")
	}()

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

	vf, err := os.Open(videoFile)
	if err != nil {
		return
	}
	defer vf.Close()

	sendMeta := func() {
		log.Println("sendMeta")

		pkt := av.Packet{}
		pkt.Type = av.Metadata
		pkt.Time = 0

		// 30fps 44.1 stereo
		pkt.Data = []byte{3, 0, 8, 100, 117, 114, 97, 116, 105, 111, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 102, 105, 108, 101, 83, 105, 122, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 119, 105, 100, 116, 104, 0, 64, 158, 0, 0, 0, 0, 0, 0, 0, 6, 104, 101, 105, 103, 104, 116, 0, 64, 144, 224, 0, 0, 0, 0, 0, 0, 12, 118, 105, 100, 101, 111, 99, 111, 100, 101, 99, 105, 100, 0, 64, 28, 0, 0, 0, 0, 0, 0, 0, 13, 118, 105, 100, 101, 111, 100, 97, 116, 97, 114, 97, 116, 101, 0, 64, 163, 136, 0, 0, 0, 0, 0, 0, 9, 102, 114, 97, 109, 101, 114, 97, 116, 101, 0, 64, 62, 0, 0, 0, 0, 0, 0, 0, 12, 97, 117, 100, 105, 111, 99, 111, 100, 101, 99, 105, 100, 0, 64, 36, 0, 0, 0, 0, 0, 0, 0, 13, 97, 117, 100, 105, 111, 100, 97, 116, 97, 114, 97, 116, 101, 0, 64, 100, 0, 0, 0, 0, 0, 0, 0, 15, 97, 117, 100, 105, 111, 115, 97, 109, 112, 108, 101, 114, 97, 116, 101, 0, 64, 229, 136, 128, 0, 0, 0, 0, 0, 15, 97, 117, 100, 105, 111, 115, 97, 109, 112, 108, 101, 115, 105, 122, 101, 0, 64, 48, 0, 0, 0, 0, 0, 0, 0, 13, 97, 117, 100, 105, 111, 99, 104, 97, 110, 110, 101, 108, 115, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 6, 115, 116, 101, 114, 101, 111, 1, 1, 0, 3, 50, 46, 49, 1, 0, 0, 3, 51, 46, 49, 1, 0, 0, 3, 52, 46, 48, 1, 0, 0, 3, 52, 46, 49, 1, 0, 0, 3, 53, 46, 49, 1, 0, 0, 3, 55, 46, 49, 1, 0, 0, 7, 101, 110, 99, 111, 100, 101, 114, 2, 0, 41, 111, 98, 115, 45, 111, 117, 116, 112, 117, 116, 32, 109, 111, 100, 117, 108, 101, 32, 40, 108, 105, 98, 111, 98, 115, 32, 118, 101, 114, 115, 105, 111, 110, 32, 51, 49, 46, 48, 46, 49, 41, 0, 0, 9}
		seqmerge.do(pkt)
	}

	sendMeta()
	t0 := time.Now()
	go audioSource(ctx, &seqmerge, t0, accStreamUrl, bus)
	go videoSource(ctx, &seqmerge, vf, t0, bus)

	restartAV := func() {
		log.Println("restart AV (media sources) !")
		bus.Publish("/src", "stop")

		time.Sleep(1 * time.Second)
		bus.Publish("/sub", "reset")
		time.Sleep(1 * time.Second)

		sendMeta()
		t0 = time.Now()
		go videoSource(ctx, &seqmerge, vf, t0, bus)
		bus.Publish("/src", "start")
	}

	cmd_ := make(chan string)
	bus.Subscribe("/pub", func(cmd string) {
		cmd_ <- cmd
	})

	ticker := time.NewTicker(24 * 60 * time.Minute) // rollover timer for infinite stream
	for {
		select {
		case cmd := <-cmd_:
			if cmd == "av:restart" {
				restartAV()
			}
		case <-ticker.C:
			restartAV()
		}
	}
}

func (st *stream) setupDownstreams(destUrl, destKey string, bus eventbus.Eventbus) error {
	runDownstreams := func() {
		log.Println("runDownstreams >")
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		wg := sync.WaitGroup{}
		for _, key := range strings.Split(destKey, ",") {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()

				if err := st.setupDownstream(destUrl+key, bus, ctx); err != nil {
					log.Println("st.setupDownstream", err)
					cancelFunc()
				}
			}(key)
		}
		wg.Wait()
	}

	for {
		runDownstreams()

		bus.Publish("/pub", "av:restart")
		time.Sleep(2 * time.Second)
	}
}

func (st *stream) setupDownstream(dest string, bus eventbus.Eventbus, ctx context.Context) error {
	fo := newFormatOpener()
	w, err := fo.Create(dest)
	if err != nil {
		log.Println("Dial Failed", err)
		return err
	}
	log.Println("Dial OK!", dest)

	c2 := w.Rtmp
	nc2 := w.NetConn
	defer nc2.Close()

	err = st.addSub(c2.CloseNotify(), c2, bus, ctx)
	return err
}
