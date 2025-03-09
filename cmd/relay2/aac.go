package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/nareix/joy5/codec/aac"
)

const (
	MAXIMUM_FRAME_SIZE = 6144

	// ADTS header size is typically 7 bytes (or 9 bytes with CRC)
	adtsHeaderSize = 7
)

type adts struct {
	aac []byte
	cfg aac.MPEG4AudioConfig
}

var track []adts

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

		track = append(track, adts{aac: frame, cfg: config})
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
