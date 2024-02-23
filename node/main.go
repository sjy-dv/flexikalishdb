package main

import (
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sjy-dv/flexikalishdb/architecture"
)

func main() {
	node := &architecture.Node{}
	node.Wg = &sync.WaitGroup{}
	node.SignalChannel = make(chan os.Signal, 1)
	node.Context, node.ContextCancel = context.WithCancel(context.Background())
	node.QueryQueueMu = &sync.Mutex{}

	node.Data = &architecture.Data{
		Map:     make(map[string][]map[string]interface{}),
		Writers: make(map[string]*sync.RWMutex),
	}

	gob.Register([]interface{}(nil))
	gob.Register(map[string]interface{}{})
	signal.Notify(node.SignalChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGABRT) // setup signal channel

	if _, err := os.Stat("./.nodeconfig"); errors.Is(err, os.ErrNotExist) {

		err = node.SetupNodeConfig() // Setup cluster node config
		if err != nil {
			os.Exit(1) // We already logged so just exit
		}
	} else {
		err = node.RenewNodeConfig()
		if err != nil {
			os.Exit(1) // We already logged so just exit
		}
	}

	err := node.SetupInitializeCDat()
	if err != nil {
		os.Exit(1) // We already logged so just exit
	}

	// Parse flags
	flag.IntVar(&node.Config.Port, "port", node.Config.Port, "port for node")
	flag.Parse()

	if len(node.Config.Observers) > 0 {
		node.ConnectToObservers()

		node.Wg.Add(1)
		go node.LostReconnectObservers() // Always attempt to reconnect to lost observers if unavailable
	}

	// If replicas are configured only then sync
	if len(node.Config.Replicas) > 0 {
		node.Wg.Add(1)
		go node.SyncOut()
	}

	if node.Config.AutomaticBackups {
		node.Wg.Add(1)
		go node.AutomaticBackup()
	}

	node.Wg.Add(1)
	go node.SignalListener() // Listen for system signals

	node.Wg.Add(1)
	go node.StartTcp() // Start listening tcp/tls with config

	node.Wg.Add(1)
	go node.SyncOutQueryQueue() // Listen for system signals

	node.Wg.Wait() // Wait for go routines to finish

	node.WriteToFile(false) // Write database data to file

	os.Exit(0) // exit
}
