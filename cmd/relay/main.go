package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type appconfig struct {
	Accounts map[string]Restream `yaml:"accounts"`
}

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
	doPubsubRtmp(":1935")
}
