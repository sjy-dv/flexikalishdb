package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sjy-dv/flexikalishdb/cluster"
)

func main() {
	cluster := &cluster.ClusterNode{}
	cluster.Wg = &sync.WaitGroup{}                                                    // create wait group
	cluster.SignalChannel = make(chan os.Signal, 1)                                   // make signal channel
	cluster.Context, cluster.ContextCancel = context.WithCancel(context.Background()) // Create context for shutdown
	cluster.ConfigMu = &sync.RWMutex{}                                                // Cluster config mutex
	cluster.UniquenessMu = &sync.Mutex{}
	// We check if a .clusterconfig file exists
	if _, err := os.Stat("./.clusterconfig"); errors.Is(err, os.ErrNotExist) {
		// .clusterconfig does not exist..

		err = cluster.SetupClusterConfig() // Setup cluster config
		if err != nil {
			os.Exit(1) // We already logged so just exit
		}
	} else { // .clusterconfig exists

		err = cluster.RenewClusterConfig()
		if err != nil {
			os.Exit(1) // We already logged so just exit
		}
	}

	// If cluster configured cluster nodes is equal to 0 inform user to add at least one node
	if len(cluster.Config.Nodes) == 0 {
		cluster.Printl("main(): You must setup nodes your clusterDB cluster to read from in your .clusterconfig file.", "INFO")
		os.Exit(0)
	}

	// We are ok to continue on and start the cluster
	signal.Notify(cluster.SignalChannel, syscall.SIGINT, syscall.SIGTERM) // Setup cluster signal channel

	// If port provided as flag use it instead of whats on config file
	flag.IntVar(&cluster.Config.Port, "port", cluster.Config.Port, "port for cluster")
	flag.Parse()

	cluster.ConnectToNodes() // Connect to configured nodes and node replicas for fast communication. We keep the connections alive

	cluster.Wg.Add(1)
	go cluster.SignalListener() // Listen to system signals

	cluster.Wg.Add(1)
	go cluster.StartTCP_TLS() // Start listening tcp/tls with setup configuration

	cluster.Wg.Add(1)
	go cluster.LostReconnect() // Always attempt to reconnect to lost nodes if unavailable

	cluster.Wg.Wait() // Wait for all go routines to finish up

	os.Exit(0) // exit
}
