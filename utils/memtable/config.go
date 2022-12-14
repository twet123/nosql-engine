package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	WalSize           int    `yaml:"wal_size"`
	MemtableSize      int    `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
}

func readConfig(filename string) Config {
	var config Config
	configData, err := ioutil.ReadFile(filename)
	if err != nil {
		configData, err = ioutil.ReadFile("config.yml")
	}
	err = yaml.Unmarshal(configData, &config)
	return config

}

func writeConfig(config Config, filename string) {
	f, err := os.Create(filename)

	marshalled, err := yaml.Marshal(config)
	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	_, err2 := f.WriteString(string(marshalled))

	if err2 != nil {
		log.Fatal(err2)
	}
	f.Close()
}
