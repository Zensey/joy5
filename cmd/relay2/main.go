package main

import (
	"errors"
	"log"
	"time"
)

func main() {
	// loadAudioTrackFromFile("jazz_swing_.aac")

	video := "winter_wonderland_background_5c64629aa854a95e28d7b95b7860a8f2.flv"

	st := stream{}
	go st.setPubFromFile(video)

	time.Sleep(10 * time.Second)

	downstream := func() error {
		dest := "rtmps://dc4-1.rtmp.t.me/s/2331156095:mZUbaZrImYgCYkwMtDJU7Q"

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
		if err := retry(100, time.Second, downstream); err != nil {
			return
		}
	}
}
