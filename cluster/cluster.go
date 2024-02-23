package cluster

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/textproto"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/ssh/terminal"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

func (cluster *ClusterNode) RenewClusterConfig() error {
	// Read .cluster config
	clusterConfigFile, err := os.ReadFile("./.clusterconfig")
	if err != nil {
		errMsg := fmt.Sprintf("main(): %d Could not open/create configuration file %s", 118, err.Error())
		cluster.Printl(errMsg, "FATAL")
		return errors.New(errMsg)
	}

	// Unmarshal config into cluster.config
	err = yaml.Unmarshal(clusterConfigFile, &cluster.Config)
	if err != nil {
		errMsg := fmt.Sprintf("main(): %d Could not unmarshal system yaml configuration ", 113) + err.Error()
		cluster.Printl(errMsg, "FATAL")
		return errors.New(errMsg)
	}

	if cluster.Config.Logging {
		cluster.LogMu = &sync.Mutex{} // Cluster log mutex
		cluster.LogFile, err = os.OpenFile("cluster.log", os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			errMsg := fmt.Sprintf("main(): %d Could not open log file ", 110) + err.Error()
			cluster.Printl(errMsg, "FATAL")
			return errors.New(errMsg)
		}
	}

	return nil
}

// SetupClusterConfig sets up default cluster config i.e .clusterconfig
func (cluster *ClusterNode) SetupClusterConfig() error {
	var err error
	cluster.Config.Port = 4701              // Default clusterDB cluster port
	cluster.Config.NodeReaderSize = 2097152 // Default node reader size of 2097152 bytes (2MB).. Pretty large json response
	cluster.Config.Host = "0.0.0.0"         // Default host of 0.0.0.0
	cluster.Config.LogMaxLines = 1000       // Default of 1000 lines then truncate/clear
	cluster.Config.Timezone = "Local"       // Default is system local time
	cluster.Config.NodeReadDeadline = 2     // Default of 2 seconds waiting for a node to respond

	// Get initial database user credentials
	fmt.Println("Before starting your clusterDB cluster you must first create an initial database user and shared cluster and node key.  This initial database user will have read and write permissions.  To add more users use curush (The clusterDB Shell) or native client.  The shared key is checked against what you setup on your nodes and used for data encryption.  All your nodes should share the same key you setup on your clusters.")
	fmt.Print("username> ")
	var username []byte
	if terminal.IsTerminal(int(syscall.Stdin)) {
		username, err = term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			errMsg := fmt.Sprintf("SetupClusterConfig(): %s", err.Error())
			cluster.Printl(errMsg, "FATAL") // No need to report status code this should be pretty apparent to troubleshoot for a user and a developer
			return errors.New(errMsg)
		}
	} else {
		reader := bufio.NewReader(os.Stdin)
		username, _, _ = reader.ReadLine()
	}

	// Relay entry with asterisks
	fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(username)))) // Relay input with *
	fmt.Println("")
	fmt.Print("password> ")

	var password []byte

	if terminal.IsTerminal(int(syscall.Stdin)) {
		password, err = term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			errMsg := fmt.Sprintf("SetupClusterConfig(): %s", err.Error())
			cluster.Printl(errMsg, "FATAL") // No need to report status code this should be pretty apparent to troubleshoot for a user and a developer
			return errors.New(errMsg)
		}
	} else {
		reader := bufio.NewReader(os.Stdin)
		password, _, _ = reader.ReadLine()
	}

	// Relay entry with asterisks
	fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(password)))) // Relay input with *
	fmt.Println("")
	fmt.Print("key> ")

	var key []byte
	if terminal.IsTerminal(int(syscall.Stdin)) {
		key, err = term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			errMsg := fmt.Sprintf("SetupClusterConfig(): %s", err.Error())
			cluster.Printl(errMsg, "FATAL")
			return errors.New(errMsg)
		}
	} else {
		reader := bufio.NewReader(os.Stdin)
		key, _, _ = reader.ReadLine()
	}

	// Relay entry with asterisks
	fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(key)))) // Relay input with *
	fmt.Println("")

	// Hash provided shared key
	hashedKey := sha256.Sum256(key)
	cluster.Config.Key = base64.StdEncoding.EncodeToString(append([]byte{}, hashedKey[:]...)) // Encode hashed key

	cluster.NewUser(string(username), string(password), "RW") // Create new user with RW permissions

	fmt.Println("")

	clusterConfigFile, err := os.OpenFile("./.clusterconfig", os.O_CREATE|os.O_RDWR, 0777) // Create .clusterconfig yaml file
	if err != nil {
		errMsg := fmt.Sprintf("SetupClusterConfig(): %d Could not open/create configuration file %s", 118, err.Error())
		cluster.Printl(errMsg, "FATAL")
		return errors.New(errMsg)
	}

	// Marshal config to yaml
	yamlData, err := yaml.Marshal(&cluster.Config)
	if err != nil {
		errMsg := fmt.Sprintf("SetupClusterConfig(): %d Could not marshal system yaml configuration %s", 114, err.Error())
		cluster.Printl(errMsg, "FATAL")
		return errors.New(errMsg)
	}

	clusterConfigFile.Write(yamlData) // Write to yaml config

	clusterConfigFile.Close() // close up cluster config

	return nil
}

// SaveConfig save cluster config such as created users and so forth on shutdown (Don't make changes to .clusterconfig when running as on shutdown changes will get overwritten)
func (cluster *ClusterNode) SaveConfig() {
	config, err := os.OpenFile(".clusterconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		cluster.Printl("SaveConfig(): Could not update config file"+err.Error(), "ERROR")
		return
	}

	defer config.Close()

	// Marshal config to yaml
	yamlConfig, err := yaml.Marshal(&cluster.Config)
	if err != nil {
		cluster.Printl(fmt.Sprintf("SaveConfig(): Could not marshal config file %s", err.Error()), "ERROR")
		os.Exit(1)
	}

	config.Write(yamlConfig)
}

// ConnectToNodes connects to configured nodes
func (cluster *ClusterNode) ConnectToNodes() {
	// Is the cluster connecting to nodes via TLS?
	if cluster.Config.TLSNode {

		// Iterate over configured nodes and connect
		for _, n := range cluster.Config.Nodes {

			// Resolve TCP addr
			tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", n.Host, n.Port))
			if err != nil {
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
				os.Exit(1)
			}

			// We will keep the node connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Configure TLS
			config := tls.Config{ServerName: n.Host}

			// Create TLS client connection
			secureConn := tls.Client(conn, &config)

			// Authenticate with node passing shared key wrapped in base64
			secureConn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cluster.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := secureConn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to node connections slice
				cluster.NodeConnections = append(cluster.NodeConnections, &NodeConnection{
					Conn:       conn,
					SecureConn: secureConn,
					Text:       textproto.NewConn(secureConn),
					Node:       n,
					Mu:         &sync.Mutex{}, // Mutex for node, again one concurrent connection can request a node at a time
					Ok:         true,          // The node is Ok :)
				})

				for _, rep := range n.Replicas {

					// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
					tcpAddrReplica, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", rep.Host, rep.Port))
					if err != nil {
						cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
						os.Exit(1)
					}

					// Dial tcp address up
					connReplica, err := net.DialTCP("tcp", nil, tcpAddrReplica)
					if err != nil {
						cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
						os.Exit(1)
					}

					// We will keep the node connection alive until shutdown
					connReplica.SetKeepAlive(true) // forever

					// Configure TLS
					configReplica := tls.Config{ServerName: rep.Host}

					// Create TLS client connection
					secureConnReplica := tls.Client(connReplica, &configReplica)

					// Authenticate with node passing shared key wrapped in base64
					secureConnReplica.Write([]byte(fmt.Sprintf("Key: %s\r\n", cluster.Config.Key)))

					// Authentication response buffer
					authBufReplica := make([]byte, 1024)

					// Read response back from node
					rReplica, _ := secureConnReplica.Read(authBufReplica[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:rReplica]), "0") {
						cluster.NodeConnections = append(cluster.NodeConnections, &NodeConnection{
							Conn:       conn,
							SecureConn: secureConnReplica,
							Text:       textproto.NewConn(secureConnReplica),
							Replica:    true,
							Node: Node{
								Host: rep.Host,
								Port: rep.Port,
							}, // Referring to main node
							Ok: true,
							Mu: &sync.Mutex{},
						})
					}

					cluster.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, connReplica.RemoteAddr().String()), "INFO")
				}

				// Report back successful connection
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, conn.RemoteAddr().String()), "INFO")
			} else {
				// Report back invalid key.
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", "Invalid key."), "FATAL")
				os.Exit(1)
			}
		}
	} else {
		for _, n := range cluster.Config.Nodes {

			// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
			tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", n.Host, n.Port))
			if err != nil {
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
				os.Exit(1)
			}

			// We will keep the node connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Authenticate with node passing shared key wrapped in base64
			conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cluster.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := conn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				cluster.NodeConnections = append(cluster.NodeConnections, &NodeConnection{
					Conn: conn,
					Mu:   &sync.Mutex{},
					Text: textproto.NewConn(conn),
					Ok:   true,
					Node: n,
				})

				for _, rep := range n.Replicas {

					// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
					tcpAddrReplica, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", rep.Host, rep.Port))
					if err != nil {
						cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")
						os.Exit(1)
					}

					// Dial tcp address up
					connReplica, err := net.DialTCP("tcp", nil, tcpAddrReplica)
					if err != nil {
						cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "FATAL")

						os.Exit(1)
					}

					// We will keep the node connection alive until shutdown
					connReplica.SetKeepAlive(true) // forever

					// Authenticate with node passing shared key wrapped in base64
					connReplica.Write([]byte(fmt.Sprintf("Key: %s\r\n", cluster.Config.Key)))

					// Authentication response buffer
					authBufReplica := make([]byte, 1024)

					// Read response back from node
					rReplica, _ := connReplica.Read(authBufReplica[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:rReplica]), "0") {
						cluster.NodeConnections = append(cluster.NodeConnections, &NodeConnection{
							Conn:    connReplica,
							Text:    textproto.NewConn(connReplica),
							Replica: true,
							Node: Node{
								Host: rep.Host,
								Port: rep.Port,
							},
							Ok: true,
							Mu: &sync.Mutex{},
						})
					}

					cluster.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, connReplica.RemoteAddr().String()), "INFO")
				}

				// Report back successful connection
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, conn.RemoteAddr().String()), "INFO")
			} else {
				// Report back invalid key
				cluster.Printl(fmt.Sprintf("ConnectToNodes(): %s", "Invalid key."), "FATAL")
				os.Exit(1)
			}

		}

	}
}

// SignalListener listens for system signals
func (cluster *ClusterNode) SignalListener() {
	defer cluster.Wg.Done()
	for {
		select {
		case sig := <-cluster.SignalChannel: // Start graceful shutdown of cluster
			cluster.Printl(fmt.Sprintf("SignalListener(): %d Received signal %s starting database cluster shutdown.", -1, sig), "INFO")
			cluster.TCPListener.Close() // Close main tcp listener
			cluster.ContextCancel()     // Send context shutdown to stop all long running go routines

			// Close all node connections
			for _, nc := range cluster.NodeConnections {
				if cluster.Config.TLSNode {
					nc.SecureConn.Close()
				} else {
					nc.Conn.Close()
				}

				nc.Text.Close()

			}

			cluster.SaveConfig() // Save config
			return
		default: // continue on waiting for signals time.Nanosecond * 1000000 is VERY efficient
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}

// StartTCP_TLS starts listening on tcp/tls on configured host and port
func (cluster *ClusterNode) StartTCP_TLS() {
	var err error           // Local error variable for StartTCP_TLS
	defer cluster.Wg.Done() // Return go routine back to wait group

	// Resolved TCPAddr struct based on configured host and port combination
	cluster.TCPAddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cluster.Config.Host, cluster.Config.Port))
	if err != nil {
		cluster.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
		cluster.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
		return
	}

	// Start listening for TCP connections on the given address
	cluster.TCPListener, err = net.ListenTCP("tcp", cluster.TCPAddr)
	if err != nil {
		cluster.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
		cluster.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
		return
	}

	for {
		conn, err := cluster.TCPListener.Accept() // Accept a new TCP connection
		if err != nil {
			cluster.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
			return
		}

		// If TLS is set to true within cluster config let's make the accepted connection secure
		if cluster.Config.TLS {
			cert, err := tls.LoadX509KeyPair(cluster.Config.TLSCert, cluster.Config.TLSKey)
			if err != nil {
				cluster.Printl(fmt.Sprintf("StartTCP_TLS(): %d Error loading X509 key pair ", 507)+err.Error(), "FATAL")
				cluster.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
				return
			}

			cluster.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}

			tlsUpgrade := tls.Server(conn, cluster.TLSConfig)
			err = tlsUpgrade.Handshake()
			if err != nil {
				conn.Close()
				continue
			} // Upgrade client connection

			conn = net.Conn(tlsUpgrade)
		}

		conn.SetReadDeadline(time.Now().Add(time.Millisecond * 150)) // We will only wait 150ms for authentication then close up their connection and go onto next
		//Expect Authentication: username\0password\n b64 encoded
		auth, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				conn.Close()
				continue // Take next connection not waiting
			}
			return
		}

		// Split at Authentication:
		authSpl := strings.Split(strings.TrimSpace(auth), "Authentication:")
		if len(authSpl) != 2 { // length not equal 2?  not good return error
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 1, "Missing authentication header.")))
			conn.Close()
			continue
		}

		// Get auth value and decode.
		authValues, err := base64.StdEncoding.DecodeString(strings.TrimSpace(authSpl[1]))
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}

		// Split AT \0 and get username and password
		authValuesSpl := strings.Split(string(authValues), "\\0")
		if len(authValuesSpl) != 2 {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}

		// Authenticate user
		_, u, err := cluster.AuthenticateUser(authValuesSpl[0], authValuesSpl[1])
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 4, err.Error()))) // no user match
			conn.Close()
			continue
		}

		// Write back to client that authentication was a success
		conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 0, "Authentication successful.")))

		cluster.Wg.Add(1)
		go cluster.HandleClientConnection(conn, u) // Handle client connection
		// .. continue on and take next client connection
	}
}

// InsertIntoNode selects one node within cluster nodes and inserts json document.
func (cluster *ClusterNode) InsertIntoNode(connection *Connection, insert string, collection string, id string) {
	var node *NodeConnection // Node connection which will be chosen randomly
	var nodeRetries int      // Amount of retries based on main node count

	nonReplicaCount := (func() int {
		i := 0
		for _, nc := range cluster.NodeConnections {
			if !nc.Replica {
				i += 1
			}
		}
		return i
	})() // Get non node replica count

	if nonReplicaCount >= 10 {
		nodeRetries = nonReplicaCount * 2 // Amount of times to retry another node if the chosen node is at peak allocation or unavailable
	} else {
		nodeRetries = 10 // Retry main node 10 times to insert
	}

	// Setting up document
	doc := make(map[string]interface{})

	// Unmarshal insert json into clusterDB document
	err := json.Unmarshal([]byte(insert), &doc)
	if err != nil {
		connection.Text.PrintfLine("%d Unmarsharable JSON insert.", 4000)
		return
	}

	doc["$id"] = id // We have already verified the id to not exist

	requestMap := make(map[string]interface{}) // request structure for node to understand

	requestMap["document"] = doc
	requestMap["action"] = "insert"

	requestMap["collection"] = collection

	jsonString, err := json.Marshal(requestMap)
	if err != nil {
		connection.Text.PrintfLine("%d Unmarsharable JSON %s", 4018, err.Error())
		return
	}

	goto query

query:
	rand.Seed(time.Now().UnixNano())

	node = cluster.NodeConnections[(0 + rand.Intn((len(cluster.NodeConnections)-1)-0+1))] // Select a random node that is not a replica

	if !node.Replica { // Make sure node connection is node a replica for inserts
		goto ok
	} else {
		goto query
	}

ok:

	if !cluster.Config.TLSNode {
		node.Conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cluster.Config.NodeReadDeadline)))
		node.Conn.SetNoDelay(true)
	} else {
		node.SecureConn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cluster.Config.NodeReadDeadline)))
	}

	goto insert

insert:
	node.Mu.Lock()
	node.Text.PrintfLine("%s", string(jsonString)) // Send the query over

	response, err := node.Text.ReadLine()
	if err != nil {
		node.Mu.Unlock()
		node.Ok = false
		if nodeRetries > -1 {
			nodeRetries -= 1
			currentNode := node
			goto findNode

		findNode:

			if nodeRetries == -1 {
				connection.Text.PrintfLine("%d No node was available for insert.", 104)
				return
			}

			node = cluster.NodeConnections[(0 + rand.Intn((len(cluster.NodeConnections)-1)-0+1))] // Pick another node, not the current one we have selected prior

			if len(cluster.Config.Nodes) > 1 {
				if fmt.Sprintf("%s:%d", node.Node.Host, node.Node.Port) == fmt.Sprintf("%s:%d", currentNode.Node.Host, currentNode.Node.Port) { // To not retry same node
					nodeRetries -= 1
					goto findNode
				} else {
					goto insert
				}
			} else {
				goto query
			}
		} else {
			connection.Text.PrintfLine("%d No node was available for insert.", 104)
			return
		}

	}

	node.Mu.Unlock()

	if strings.Contains(response, "\"statusCode\":\"100\"") {
		// Node was at peak allocation.
		cluster.Printl(fmt.Sprintf("InsertIntoNode(): %s was at peak allocation.  Consider providing more memory to node.", node.Conn.RemoteAddr().String()), "WARN")
		// Picking another node and trying again
		if nodeRetries > -1 {
			nodeRetries -= 1
			goto query
		} else {
			node.Ok = false
			connection.Text.PrintfLine("%d No node was available for insert.", 104)
			return
		}
	}

	connection.Text.PrintfLine(response)

}
