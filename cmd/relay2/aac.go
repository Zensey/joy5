package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nareix/joy5/av"
	"github.com/nareix/joy5/codec/aac"
)

const (
	MAXIMUM_FRAME_SIZE = 6144

	// ADTS header size is typically 7 bytes (or 9 bytes with CRC)
	adtsHeaderSize = 7
)

type adtsPkt struct {
	data   []byte
	config aac.MPEG4AudioConfig
}

var track []adtsPkt

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
			pktDuration := aac.PacketDuration(track[i].config, nil)
			pkt.Time = pktTimePrev + pktDuration
			pkt.Data = track[i].data

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

func loadAudioTrackFromFile(fname string) {
	file, err := os.Open(fname) // Replace with your stream source
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	adtsHdr := make([]byte, adtsHeaderSize)
	adtsHdr[0] = 0xff
	adtsHdr[1] = 0xf1

	for {
		err := findADTSHeader(adtsHdr, reader)
		if err != nil {
			fmt.Println("ADTS header found!")
			break
		}

		config, hdrlen, framelen, samples, err := aac.ParseADTSHeader(adtsHdr)
		if err != nil {
			fmt.Println("ADTS header parse error:", err)
			fmt.Println("ADTS:", adtsHdr)

			continue
			// ? make a delay
		}
		fmt.Println(config, hdrlen, framelen, samples, err)

		frame := make([]byte, framelen)
		copy(frame[0:7], adtsHdr)
		_, err = io.ReadFull(reader, frame[7:])
		if err != nil {
			fmt.Println("Read frame error:", err)
			continue
		}

		track = append(track, adtsPkt{data: frame, config: config})
	}
}

func findADTSHeader(buf []byte, reader *bufio.Reader) error {
	for i := 0; i < MAXIMUM_FRAME_SIZE; i++ {
		b1, err := reader.ReadByte()
		if err == io.EOF {
			return err
		}
		if b1 == 0xff {
			b2, err := reader.ReadByte()
			if err == io.EOF {
				return err
			}
			if b2 == 0xf1 {
				_, err := io.ReadFull(reader, buf[2:])
				if err == io.EOF {
					return err
				}
				return nil
			}
		}
	}
	return errors.New("adts not found")
}

func getEmptyAAC() []byte {
	return []byte{33, 16, 4, 96, 140, 28}
}
