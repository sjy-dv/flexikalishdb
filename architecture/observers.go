package architecture

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/textproto"
	"os"
	"strings"
	"time"
)

// ConnectToObservers connects to Observer listeners
// node will send an initial Key: SHAREDKEY\r\n
// This is read by the Observer and accepted at which point the Node and Observer can communicate.
func (node *Node) ConnectToObservers() {
	// Is the node connecting to observers via TLS?
	if node.Config.TLSObservers {

		// Iterate over configured observers and connect
		for _, o := range node.Config.Observers {

			// Resolve TCP addr
			tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", o.Host, o.Port))
			if err != nil {
				node.PLof(fmt.Sprintf("ConnectToObservers(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				node.PLof(fmt.Sprintf("ConnectToObservers(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// We will keep the observer connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Configure TLS
			config := tls.Config{ServerName: o.Host}

			// Create TLS client connection
			secureConn := tls.Client(conn, &config)

			// Authenticate with node passing shared key wrapped in base64
			secureConn.Write([]byte(fmt.Sprintf("Key: %s\r\n", node.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := secureConn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				node.ObserverConnections = append(node.ObserverConnections, &ObserverConnection{
					Conn:       conn,
					SecureConn: secureConn,
					Text:       textproto.NewConn(secureConn),
					Ok:         true,
					Observer:   o,
				})

				// Report back successful connection
				node.PLof(fmt.Sprintf("ConnectToObservers(): %d Observer connection established with %s", 224, conn.RemoteAddr().String()), "INFO")
			} else {
				// Report back invalid key.
				node.PLof(fmt.Sprintf("ConnectToObservers(): %s", "Invalid key."), "ERROR")
				os.Exit(1)
			}
		}
	} else {
		for _, o := range node.Config.Observers {

			// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
			tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", o.Host, o.Port))
			if err != nil {
				node.PLof(fmt.Sprintf("ConnectToObservers(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				node.PLof(fmt.Sprintf("ConnectToObservers(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// We will keep the observer connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Authenticate with node passing shared key wrapped in base64
			conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", node.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := conn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				node.ObserverConnections = append(node.ObserverConnections, &ObserverConnection{
					Conn:     conn,
					Text:     textproto.NewConn(conn),
					Ok:       true,
					Observer: o,
				})

				// Report back successful connection
				node.PLof(fmt.Sprintf("ConnectToObservers(): %d Observer connection established with %s", 224, conn.RemoteAddr().String()), "INFO")
			} else {
				// Report back invalid key
				node.PLof(fmt.Sprintf("ConnectToObservers(): %s", "Invalid key."), "ERROR")
				os.Exit(1)
			}

		}

	}
}

// SendToObservers transmits a new insert, update, or delete event to all configured observers
func (node *Node) SendToObservers(jsonStr string) {
	for i, oc := range node.ObserverConnections {
		oc.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		err := oc.Text.PrintfLine(jsonStr)
		if err != nil {
			node.PLof(fmt.Sprintf("SendToObservers(): %d Observer %s was unavailable during relay.", 218, fmt.Sprintf("%s:%d", oc.Observer.Host, oc.Observer.Port)), "WARNING")
			node.ObserverConnections[i].Ok = false
			continue
		}
	}
}

// LostReconnectObservers connects to lost observers or will try to.
func (node *Node) LostReconnectObservers() {
	defer node.Wg.Done() // Defer to return to waitgroup

	for {
		if node.Context.Err() != nil { // On signal break out of for loop
			break
		}

		for i, oc := range node.ObserverConnections { // Iterate over observer connections
			if !oc.Ok { // Check if observer connection is not ok
				if node.Config.TLSObservers { // Is TLS observer configured?

					// Resolve TCP addr
					tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", oc.Observer.Host, oc.Observer.Port))
					if err != nil {
						node.PLof(fmt.Sprintf("LostReconnectObservers(): %s", err.Error()), "ERROR")
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// Dial tcp address up
					conn, err := net.DialTCP("tcp", nil, tcpAddr)
					if err != nil {
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// We will keep the node connection alive until shutdown
					conn.SetKeepAlive(true) // forever

					// Configure TLS
					config := tls.Config{ServerName: oc.Observer.Host} // Either ServerName or InsecureSkipVerify will do it

					// Create TLS client connection
					secureConn := tls.Client(conn, &config)

					// Authenticate with node passing shared key wrapped in base64
					secureConn.Write([]byte(fmt.Sprintf("Key: %s\r\n", node.Config.Key)))

					// Authentication response buffer
					authBuf := make([]byte, 1024)

					// Read response back from node
					r, _ := secureConn.Read(authBuf[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:r]), "0") {

						node.ObserverConnections[i] = &ObserverConnection{
							Conn:       conn,
							SecureConn: secureConn,
							Text:       textproto.NewConn(secureConn),
							Observer:   oc.Observer,
							Ok:         true,
						}

						node.PLof(fmt.Sprintf("LostReconnectObservers(): %d Reconnected to lost observer connection ", 117)+fmt.Sprintf("%s:%d", oc.Observer.Host, oc.Observer.Port), "INFO")
						time.Sleep(time.Nanosecond * 1000000)

					}
				} else {

					// Resolve TCP addr
					tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", oc.Observer.Host, oc.Observer.Port))
					if err != nil {
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// Dial tcp address up
					conn, err := net.DialTCP("tcp", nil, tcpAddr)
					if err != nil {
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// We will keep the observer connection alive until shutdown
					conn.SetKeepAlive(true) // forever

					// Authenticate with node passing shared key wrapped in base64
					conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", node.Config.Key)))

					// Authentication response buffer
					authBuf := make([]byte, 1024)

					// Read response back from node
					r, _ := conn.Read(authBuf[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:r]), "0") {

						node.ObserverConnections[i] = &ObserverConnection{
							Conn:     conn,
							Text:     textproto.NewConn(conn),
							Observer: oc.Observer,
							Ok:       true,
						}

						node.PLof(fmt.Sprintf("LostReconnectObservers(): %d Reconnected to lost observer connection ", 117)+fmt.Sprintf("%s:%d", oc.Observer.Host, oc.Observer.Port), "INFO")
						time.Sleep(time.Nanosecond * 1000000)
					}

					time.Sleep(time.Nanosecond * 1000000)

				}
			}
		}
		time.Sleep(time.Nanosecond * 1000000)
	}

}
