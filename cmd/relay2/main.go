package main

import (
	"errors"
	"flag"
	"log"
	"strings"
	"time"
)

func main() {
	destUrl := flag.String("url", "rtmp://localhost:1935/live", "RTMP destination ")
	destKey := flag.String("key", "", "RTMP destination key")
	video := flag.String("video", "output.flv", "Video file")
	accStreamUrl := flag.String("acc-stream", "http://localhost:8000/stream.aac", "ACC stream url")

	flag.Parse()
	if *destKey == "" {
		return
	}

	st := stream{}
	go st.setPubFromFile(*video, *accStreamUrl)

	time.Sleep(2 * time.Second)

	setupDownstream := func() error {
		if !strings.HasSuffix(*destUrl, "/") {
			*destUrl += "/"
		}
		dest := *destUrl + *destKey

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

		st.addSub(c2.CloseNotify(), c2)
		return errors.New("disconnect")
	}

	for {
		if err := retry(1000, time.Second, setupDownstream); err != nil {
			return
		}
	}
}
