package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nareix/joy5/av"
	"github.com/nareix/joy5/format"
	"github.com/nareix/joy5/format/flv"
)

func main() {
	src := "winter_wonderland_background_5c64629aa854a95e28d7b95b7860a8f2.flv"
	fr, err := os.Open(src)
	if err != nil {
		return
	}
	defer fr.Close()

	fwd := "rtmps://dc4-1.rtmp.t.me/s/2331156095:mZUbaZrImYgCYkwMtDJU7Q"
	fo := newFormatOpener()
	var w *format.Writer
	if w, err = fo.Create(fwd); err != nil {
		log.Println("DialFailed", err)
		return
	}
	c2 := w.Rtmp
	nc2 := w.NetConn
	defer nc2.Close()
	log.Println("DialOK")


	offset := time.Duration(0)
	for {
		fr.Seek(0, 0)
		r := flv.NewDemuxer(fr)

		duration := time.Duration(0)
		for {
			pkt, err := r.ReadPacket()
			if err != nil {
				log.Println(err)
				break
			}
			if pkt.Type == av.Metadata {
				// omit metadata packet
				continue
			}

			dif := pkt.Time-duration
			time.Sleep(dif)

			duration = pkt.Time
			pkt.Time += offset
			fmt.Println("pkt", pkt.String(), duration, offset)

			if err = c2.WritePacket(pkt); err != nil {
				log.Println(err)
				break
			}
		}
		duration += time.Millisecond * 10
		offset += duration
	}
}
