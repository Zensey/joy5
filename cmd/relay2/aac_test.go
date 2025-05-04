package main

import (
	"log"
	"testing"

	"github.com/nareix/joy5/codec/aac"
)

func Test_emptyPacket(t *testing.T) {

	var emptyAAC = getEmptyAAC()

	adtsHdr := make([]byte, adtsHeaderSize)
	adtsHdr[0] = 0xff
	adtsHdr[1] = 0xf1
	copy(adtsHdr[2:], emptyAAC)

	log.Println(adtsHdr)

	config, hdrlen, framelen, samples, err := aac.ParseADTSHeader(adtsHdr)
	if err != nil {
		log.Println("ADTS header parse error:", err)
		log.Println("ADTS:", adtsHdr)
		t.Error(err)
		return
	}
	log.Println("config, hdrlen, framelen, samples ", config, hdrlen, framelen, samples)

	pktDuration := aac.PacketDuration(config, nil)
	log.Println("pktDuration>", pktDuration)

}
