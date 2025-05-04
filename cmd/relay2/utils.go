package main

import (
	"fmt"
	"log"
	"time"
)

const defaultMaxInterval = 2 * 60 * time.Second

// var (
// 	errOK = errors.New("")
// )

func retry(maxAttempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; i < maxAttempts; i++ {
		fmt.Println("This is attempt number", i+1)

		// calling the important function
		err = f()
		if err != nil {
			log.Printf("error occured after attempt number %d: %s", i+1, err.Error())
			log.Println("sleeping for: ", sleep.String())
			time.Sleep(sleep)

			if sleep < defaultMaxInterval {
				sleep *= 2
			}
			if sleep > defaultMaxInterval {
				sleep = defaultMaxInterval
			}
			continue
		}
		break
	}
	return err
}

func durationFromFloat64(f float64) time.Duration {
	return time.Duration(f * 1e9)
}
