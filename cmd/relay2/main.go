package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/danielhookx/eventbus"
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
	if !strings.HasSuffix(*destUrl, "/") {
		*destUrl += "/"
	}

	bus := eventbus.New()

	st := stream{}
	go st.setPubFromFile(*video, *accStreamUrl, bus)
	go st.setupDownstreams(*destUrl, *destKey, bus)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}
