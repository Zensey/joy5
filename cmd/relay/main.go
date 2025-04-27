package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/nareix/joy5/cmd/relay/handlers"
	"gopkg.in/yaml.v3"
)

var (
	config appconfig
)

func readConfigs() {
	yamlFile, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		panic(err)
	}
}

func main() {
	readConfigs()
	svc, _ := doPubsubRtmp(":1935")

	h := handlers.SetHttpHandlers(svc)
	go http.ListenAndServe(":8181", h)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	svc.Stop()
}
