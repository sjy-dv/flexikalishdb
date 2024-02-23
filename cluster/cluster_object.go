package cluster

import (
	"context"
	"crypto/tls"
	"net"
	"net/textproto"
	"os"
	"sync"
)

type ClusterNode struct {
	TCPAddr         *net.TCPAddr       // TCPAddr represents the address of the clusters TCP end point
	TCPListener     *net.TCPListener   // TCPListener is the cluster TCP network listener.
	Wg              *sync.WaitGroup    // Cluster WaitGroup waits for all goroutines to finish up
	NodeConnections []*NodeConnection  // Configured and forever connected node connections until shutdown.
	SignalChannel   chan os.Signal     // Catch operating system signal
	Config          Config             // Cluster config
	TLSConfig       *tls.Config        // Cluster TLS config if TLS is true
	ContextCancel   context.CancelFunc // For gracefully shutting down
	ConfigMu        *sync.RWMutex      // Cluster config mutex
	Context         context.Context    // Main looped go routine context.  This is for listeners, event loops and so forth
	LogMu           *sync.Mutex        // Log file mutex (only if logging enabled)
	LogFile         *os.File           // Opened log file (only if logging enabled)
	UniquenessMu    *sync.Mutex        // If many connections are inserting the same document there is a chance for the same document so if uniqueness is required there is a lock on insert on the cluster.  The cluster does lock nodes on reads but as this is a concurrent system 2 connections at the same time can cause 2 of the same records without this lock.
}

// NodeConnection is the cluster connected to a node as a client.
type NodeConnection struct {
	Conn       *net.TCPConn    // Net connection
	SecureConn *tls.Conn       // Secure connection with TLS
	Text       *textproto.Conn // For writing and reading
	Mu         *sync.Mutex     // Multiple connections shouldn't hit the same node without the node being locked
	Replica    bool            // is node replica?
	Ok         bool            // Is node ok?
	Node       Node            // The underlaying Node for connection
}

// Connection is the main TCP connection struct for cluster
type Connection struct {
	Text *textproto.Conn        // Text is used for reading and writing
	Conn net.Conn               // net.Conn is a generic stream-oriented network connection.
	User map[string]interface{} // Authenticated user
}

// Config is the CursusDB cluster config struct
type Config struct {
	Nodes            []Node   `yaml:"nodes"`                         // Node host/ips
	Host             string   `yaml:"host"`                          // Cluster host
	TLSNode          bool     `default:"false" yaml:"tls-node"`      // Connects to nodes with tls.  Nodes MUST be using tls in-order to set this to true.
	TLSCert          string   `yaml:"tls-cert"`                      // Location to TLS cert
	TLSKey           string   `yaml:"tls-key"`                       // Location to TLS key
	TLS              bool     `default:"false" yaml:"tls"`           // TLS on or off ?
	Port             int      `yaml:"port"`                          // Cluster port
	Key              string   `yaml:"key"`                           // Shared key - this key is used to encrypt data on all nodes and to authenticate with a node.
	Users            []string `yaml:"users"`                         // Array of encoded users
	NodeReaderSize   int      `yaml:"node-reader-size"`              // How large of a response buffer can the cluster handle
	LogMaxLines      int      `yaml:"log-max-lines"`                 // At what point to clear logs.  Each log line start's with a [UTC TIME] LOG DATA
	JoinResponses    bool     `default:"true" yaml:"join-responses"` // Joins all nodes results limiting at n
	Logging          bool     `default:"false" yaml:"logging"`       // Log to file ?
	LogQuery         bool     `default:"false" yaml:"log-query"`     // Log incoming queries
	Timezone         string   `default:"Local" yaml:"timezone"`      // i.e America/Chicago default is local system time.  On the cluster we use the Timezone for logging purposes.
	NodeReadDeadline int      `yaml:"node-read-deadline"`            // Amount of seconds to wait for a node or node replica.  Default is 2 seconds
}

// Node is a cluster node
type Node struct {
	Host     string        `yaml:"host"` // Cluster node host i.e 0.0.0.0 or cluster0.example.com
	Port     int           `yaml:"port"` // Cluster node port default for a cluster node is 4701
	Replicas []NodeReplica // Cluster node replicas of configured.  If node becomes unavailable where to go to instead.
}

// NodeReplica is a replica of original node.  Used in-case active node is not available
type NodeReplica struct {
	Host string `yaml:"host"` // Cluster node replica host i.e 0.0.0.0 or cluster0.example.com
	Port int    `yaml:"port"` // Default cluster node port of 4701 but can be configured
}

var (
	reservedKeys = []string{`"count":`, `"$id":`, `"$indx":`, `"in":`, `"not like":`, `"!like":`,
		`"where":`, `"*":`, `"chan":`, `"const":`, `"continue":`, `"defer":`,
		`"else":`, `"fallthrough":`, `"func":`, `"go":`, `"goto":`, `"if":`,
		`"interface":`, `"map":`, `"select":`, `"struct":`, `"switch":`,
		`"var":`, `"false":`, `"true":`, `"uint8":`, `"uint16":`, `"uint32":`,
		`"uint64":`, `"int8":`, `"int16":`, `"int32":`, `"int64":`, `"float32":`,
		`"float64":`, `"complex64":`, `"complex128":`, `"byte":`, `"rune":`,
		`"uint":`, `"int":`, `"uintptr":`, `"string":`} // Reserved document keys
	reservedSymbols = []string{`"==":`, `"&&":`, `"||":`, `">":`, `"<":`, `"=>":`, `"=<":`, `"=":`} // These are reserved symbols in which cannot be used as document keys
)
