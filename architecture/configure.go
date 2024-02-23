package architecture

import (
	"context"
	"crypto/tls"
	"net"
	"net/textproto"
	"os"
	"sync"
)

type Node struct {
	TCPAddr             *net.TCPAddr             // TCPAddr represents the address of the nodes TCP end point
	TCPListener         *net.TCPListener         // TCPListener is the node TCP network listener.
	Wg                  *sync.WaitGroup          // Node WaitGroup waits for all goroutines to finish up
	SignalChannel       chan os.Signal           // Catch operating system signal
	Config              Config                   // Node  config
	TLSConfig           *tls.Config              // Node TLS config if TLS is true
	ContextCancel       context.CancelFunc       // For gracefully shutting down
	ConfigMu            *sync.RWMutex            // Node config mutex
	Data                *Data                    // Node data
	Context             context.Context          // Main looped go routine context.  This is for listeners, event loops and so forth
	LogMu               *sync.Mutex              // Log file mutex
	LogFile             *os.File                 // Opened log file
	ObserverConnections []*ObserverConnection    // ObserverConnections
	QueryQueue          []map[string]interface{} // QueryQueue is a queue of queries coming in from cluster(s) and is synced to a file which is encrypted every 100ms
	QueryQueueMu        *sync.Mutex              // QueryQueue mutex
}

// Config is the cluster config struct
type Config struct {
	Replicas                    []Replica  `yaml:"replicas"`                                 // Replicas are replica of this current node
	TLSCert                     string     `yaml:"tls-cert"`                                 // TLS cert path
	TLSKey                      string     `yaml:"tls-key"`                                  // TLS cert key
	Host                        string     `yaml:"host"`                                     // Node host i.e 0.0.0.0 usually
	TLS                         bool       `default:"false" yaml:"tls"`                      // Use TLS?
	Port                        int        `yaml:"port"`                                     // Node port
	Key                         string     `yaml:"key"`                                      // Key for a cluster to communicate with the node and also used to resting data.
	MaxMemory                   uint64     `yaml:"max-memory"`                               // Default 10240MB = 10 GB (1024 * 10)
	LogMaxLines                 int        `yaml:"log-max-lines"`                            // At what point to clear logs.  Each log line start's with a [UTC TIME] LOG DATA
	Logging                     bool       `default:"false" yaml:"logging"`                  // Log to file ?
	ReplicationSyncTime         int        `yaml:"replication-sync-time"`                    // in minutes default is every 10 minutes
	ReplicationSyncTimeout      int        `yaml:"replication-sync-timeout"`                 // As your node grows in size you may want to increase.  Default is 10 minutes.
	TLSReplication              bool       `default:"false" yaml:"tls-replication"`          // If your cluster node replicas are running TLS then configure this to true
	AutomaticBackups            bool       `default:"false" yaml:"automatic-backups"`        // If for some reason a .cdat gets corrupt you can choose to have the system save a state of your .cdat file every set n amount of time.  (default is every 8 hours(480 minutes) to make a backup of your nodes data under BackupsDirectory(which the system will create inside your binary executable location) files are named like so .cdat_YYMMDDHHMMSS in your set timezone
	AutomaticBackupTime         int        `yaml:"automatic-backup-time"`                    // Automatic node backup time.  Default is 8 (hours)
	AutomaticBackupCleanup      bool       `default:"false" yaml:"automatic-backup-cleanup"` // If set true node will clean up backups that are older than AutomaticBackupCleanupTime days old
	AutomaticBackupCleanupHours int        `yaml:"automatic-backup-cleanup-hours"`           // Clean up old .cdat backups that are n amount hours old only used if AutomaticBackups is set true default is 12 hours
	Timezone                    string     `default:"Local" yaml:"timezone"`                 // i.e America/Chicago default is local system time
	Observers                   []Observer `yaml:"observers"`                                // Observer servers listening for realtime node events (insert,update,delete).  node if configured will relay successful inserts, updates, and deletes to all Observer(s)
	TLSObservers                bool       `yaml:"tls-observers"`                            // Set whether your Observers are listening on tls or not
	BackupsDirectory            string     `yaml:"backups-directory"`                        // Backups directory by default is in the execution directory /backups/ Whatever is provided the system will create the director(ies) if they doesn't exist.
}

// Replica is a cluster node that current node data will be replicated/synced to
type Replica struct {
	Host string `yaml:"host"` // Host of replica i.e an ip or fqdn
	Port int    `yaml:"port"` // Port of replica
}

// Observer is a FlexiKalish Observer which listens for realtime node events.
type Observer struct {
	Host string `yaml:"host"` // Host of Observer i.e an ip or fqdn
	Port int    `yaml:"port"` // Port of Observer
}

// ObserverConnection is Node to Observer TCP or TLS connection
type ObserverConnection struct {
	Conn       *net.TCPConn    // Net connection
	SecureConn *tls.Conn       // Secure connection with TLS
	Text       *textproto.Conn // For writing and reading
	Ok         bool            // Is observer ok?
	Observer   Observer        // The underlying Observer for connection
}

// Data is the node data struct
type Data struct {
	Map     map[string][]map[string]interface{} // Data hash map
	Writers map[string]*sync.RWMutex            // Collection writers
}

// Global variables
