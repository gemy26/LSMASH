package main

import (
	"lsmash/config"
	engine "lsmash/internal/engine"
)

func main() {
	cfg := config.DefaultConfig()
	eng, err := engine.CreateEngine(cfg)
	if err != nil {
		panic(err)
	}
	server := engine.NewServer("localhost:3030")
	server.Engine = eng
	err = server.Start()
	if err != nil {
		return
	}
}
