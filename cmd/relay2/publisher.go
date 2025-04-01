package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nareix/joy5/av"
	"github.com/nareix/joy5/codec/aac"
	"github.com/nareix/joy5/format/flv"
	"github.com/nareix/joy5/format/flv/flvio"
)

func audioSource(ctx context.Context, ctrl chan string, seqmerge *mergeSeqhdr, t0 time.Time, url string) {
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

			case cmd := <-ctrl:
				log.Println("cmd", cmd)
				if cmd == "stop" {
					skip = true
				}
				if cmd == "start" {
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
				// break
				return err
			}

			config, _, framelen, _, err := aac.ParseADTSHeader(adtsHdr)
			if err != nil {
				log.Println("ADTS header parse error:", err)
				log.Println("ADTS:", adtsHdr)
				// continue
				return err
			}

			frame := make([]byte, framelen)
			copy(frame[0:7], adtsHdr)
			_, err = io.ReadFull(reader, frame[7:])
			if err != nil {
				log.Println("Read frame error:", err)
				// continue
				return err
			}

			pkt := av.Packet{}
			pkt.Type = av.AAC
			pkt.Time = offset
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

func video(ctx context.Context, ctrl chan string, seqmerge *mergeSeqhdr, fr *os.File, t0 time.Time) {
	lag := time.Duration(0)
	offset := time.Duration(0)
	videoDuration := time.Duration(0)

	for {
		fr.Seek(0, 0)
		r := flv.NewDemuxer(fr)
		pktTime := time.Duration(0)
		pktTimePrev := time.Duration(0)

		for {
			select {
			case <-ctx.Done():
				log.Println("videoSource Done!")
				return

			case cmd := <-ctrl:
				log.Println("video > cmd", cmd)
				return

			default:
			}

			pkt, err := r.ReadPacket()
			if err != nil {
				if err != io.EOF {
					log.Println(err)
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

func (s *stream) setPubFromFile(src string, accStreamUrl string) {
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

	vfsrc, err := os.Open(src)
	if err != nil {
		return
	}
	defer vfsrc.Close()

	t0 := time.Now()
	ctrA := make(chan string)
	ctrV := make(chan string)

	pkt := av.Packet{}
	pkt.Type = av.Metadata
	pkt.Time = 0

	// 30fps 44.1 stereo
	pkt.Data = []byte{3, 0, 8, 100, 117, 114, 97, 116, 105, 111, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 102, 105, 108, 101, 83, 105, 122, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 119, 105, 100, 116, 104, 0, 64, 158, 0, 0, 0, 0, 0, 0, 0, 6, 104, 101, 105, 103, 104, 116, 0, 64, 144, 224, 0, 0, 0, 0, 0, 0, 12, 118, 105, 100, 101, 111, 99, 111, 100, 101, 99, 105, 100, 0, 64, 28, 0, 0, 0, 0, 0, 0, 0, 13, 118, 105, 100, 101, 111, 100, 97, 116, 97, 114, 97, 116, 101, 0, 64, 163, 136, 0, 0, 0, 0, 0, 0, 9, 102, 114, 97, 109, 101, 114, 97, 116, 101, 0, 64, 62, 0, 0, 0, 0, 0, 0, 0, 12, 97, 117, 100, 105, 111, 99, 111, 100, 101, 99, 105, 100, 0, 64, 36, 0, 0, 0, 0, 0, 0, 0, 13, 97, 117, 100, 105, 111, 100, 97, 116, 97, 114, 97, 116, 101, 0, 64, 100, 0, 0, 0, 0, 0, 0, 0, 15, 97, 117, 100, 105, 111, 115, 97, 109, 112, 108, 101, 114, 97, 116, 101, 0, 64, 229, 136, 128, 0, 0, 0, 0, 0, 15, 97, 117, 100, 105, 111, 115, 97, 109, 112, 108, 101, 115, 105, 122, 101, 0, 64, 48, 0, 0, 0, 0, 0, 0, 0, 13, 97, 117, 100, 105, 111, 99, 104, 97, 110, 110, 101, 108, 115, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 6, 115, 116, 101, 114, 101, 111, 1, 1, 0, 3, 50, 46, 49, 1, 0, 0, 3, 51, 46, 49, 1, 0, 0, 3, 52, 46, 48, 1, 0, 0, 3, 52, 46, 49, 1, 0, 0, 3, 53, 46, 49, 1, 0, 0, 3, 55, 46, 49, 1, 0, 0, 7, 101, 110, 99, 111, 100, 101, 114, 2, 0, 41, 111, 98, 115, 45, 111, 117, 116, 112, 117, 116, 32, 109, 111, 100, 117, 108, 101, 32, 40, 108, 105, 98, 111, 98, 115, 32, 118, 101, 114, 115, 105, 111, 110, 32, 51, 49, 46, 48, 46, 49, 41, 0, 0, 9}
	seqmerge.do(pkt)

	go audioSource(ctx, ctrA, &seqmerge, t0, accStreamUrl)
	go video(ctx, ctrV, &seqmerge, vfsrc, t0)

	for {
		time.Sleep(24 * 60 * time.Minute) // 4h rollover
		ctrV <- "stop"
		ctrA <- "stop"

		// wait for sub idle, i.e. output que is empty
		log.Println("wait for sub idle")
		for !sp.gc.subIdle {
			log.Println("wait >")
			time.Sleep(10 * time.Millisecond)
		}

		t0 = time.Now()
		go video(ctx, ctrV, &seqmerge, vfsrc, t0)
		ctrA <- "start"
	}
}
