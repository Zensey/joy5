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

func audioSourceFromAAC(ctx context.Context, seqmerge *mergeSeqhdr) {
	t0 := time.Now()
	lag := time.Duration(0)
	offset := time.Duration(0)
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pktTimePrev := time.Duration(0)
		for i := 0; i < len(track); i++ {
			pkt := av.Packet{}
			pkt.Type = av.AAC
			pktDuration := aac.PacketDuration(track[i].cfg, nil)
			pkt.Time = pktTimePrev + pktDuration
			pkt.Data = track[i].aac
			// if ASeqHdr will be required, apply aac.WriteMPEG4AudioConfig() to adts frame config

			delay := pktDuration - lag
			pktTimePrev = pkt.Time
			pkt.Time += offset

			if delay > 0 {
				time.Sleep(delay)
			}
			seqmerge.do(pkt)

			lag = time.Since(t0) - pkt.Time
			// log.Printf("%v %-12v %-11v %-12v", av.PacketTypeString[pkt.Type], pkt.Time, lag, i)
		}
		offset += pktTimePrev
	}
}

func audioSource(ctx context.Context, ctrl chan string, seqmerge *mergeSeqhdr, t0 time.Time) {
	defer func() {
		log.Println("audioSource2 exit")
	}()

	offset := time.Duration(0)

	streamURL := "http://relay.publicdomainradio.org:80/jazz_swing.aac"
	resp, err := http.Get(streamURL)
	if err != nil {
		fmt.Println("Error connecting to stream:", err)
		return
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)

	ap := av.Packet{}
	ap.Type = av.AACDecoderConfig
	ap.Time = 0
	ap.Data = bbAACDecoderConfig.Bytes()

	seqmerge.do(ap)

	adtsHdr := make([]byte, adtsHeaderSize, 7)
	adtsHdr[0] = 0xff
	adtsHdr[1] = 0xf1

	skip := false
	for {
		select {
		case <-ctx.Done():
			log.Println("audioSource2 Done!")
			return

		case cmd := <-ctrl:
			log.Println("cmd", cmd)
			if cmd == "stop" {
				skip = true
			}
			if cmd == "start" {
				skip = false
				offset = time.Duration(0)
			}
		default:
		}

		err := findADTSHeader(adtsHdr, reader)
		if err != nil {
			fmt.Println("No ADTS header found!")
			break
		}

		config, _, framelen, _, err := aac.ParseADTSHeader(adtsHdr)
		if err != nil {
			fmt.Println("ADTS header parse error:", err)
			fmt.Println("ADTS:", adtsHdr)

			continue
		}

		frame := make([]byte, framelen)
		copy(frame[0:7], adtsHdr)
		_, err = io.ReadFull(reader, frame[7:])
		if err != nil {
			fmt.Println("Read frame error:", err)
			continue
		}

		pkt := av.Packet{}
		pkt.Type = av.AAC
		pkt.Time = offset
		pkt.Data = frame
		// pkt.ASeqHdr = bbAACDecoderConfig.Bytes()

		walltime := time.Since(t0)
		if pkt.Time > walltime {
			log.Println("audio> drop", pkt.Time, walltime)

			// drop packets which came due to pre-beginning (in the beginning)
			continue
		}

		if !skip {
			seqmerge.do(pkt)
		}
		offset += aac.PacketDuration(config, nil)
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
				// log.Println("video duration", videoDuration)
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
			// log.Printf(">> %v %-12v %-12v", av.PacketTypeString[pkt.Type], pkt.Time, lag)
		}

		offset += videoDuration
		rest := videoDuration - pktTimePrev - lag
		time.Sleep(rest)
		lag = time.Since(t0) - offset
	}
}

func (s *stream) setPubFromFile(src string) {
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

	fr, err := os.Open(src)
	if err != nil {
		return
	}
	defer fr.Close()

	t0 := time.Now()
	ctrV := make(chan string)
	ctrA := make(chan string)

	// go audioSourceFromAAC(ctx, &seqmerge)
	go audioSource(ctx, ctrA, &seqmerge, t0)
	go video(ctx, ctrV, &seqmerge, fr, t0)

	for {
		time.Sleep(240 * time.Minute) // 4h rollover
		ctrV <- "stop"
		ctrA <- "stop"

		// wait for sub idle, i.e. output que is empty
		log.Println("wait for sub idle")
		for !sp.gc.subIdle {
			log.Println("wait >")
			time.Sleep(10 * time.Millisecond)
		}

		t0 = time.Now()
		go video(ctx, ctrV, &seqmerge, fr, t0)
		ctrA <- "start"
	}
}
