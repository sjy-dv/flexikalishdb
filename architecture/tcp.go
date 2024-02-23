package architecture

import (
	"bufio"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

func (node *Node) StartTcp() {
	var err error
	defer node.Wg.Done()
	node.TCPAddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", node.Config.Host, node.Config.Port))
	if err != nil {
		node.PLof("StartTCP_TLS(): "+err.Error(), "FATAL")
		node.SignalChannel <- os.Interrupt
		return
	}
	node.PLof(fmt.Sprintf("StartTcp(): Action_startup in %v", node.TCPAddr), "INFO")
	// Start listening for TCP connections on the given address
	node.TCPListener, err = net.ListenTCP("tcp", node.TCPAddr)
	if err != nil {
		node.PLof("StartTCP_TLS(): "+err.Error(), "FATAL")
		node.SignalChannel <- os.Interrupt
		return
	}

	go func() {
		time.Sleep(time.Millisecond * 20)
		node.StartRunQueryQueue() // Run any queries that were left behind due to failure or crisis
	}()

	for {
		conn, err := node.TCPListener.Accept()
		if err != nil {
			node.SignalChannel <- os.Interrupt
			return
		}

		// If TLS is set to true within config let's make the connection secure
		if node.Config.TLS {
			cert, err := tls.LoadX509KeyPair(node.Config.TLSCert, node.Config.TLSKey)
			if err != nil {
				node.PLof(fmt.Sprintf("StartTCP_TLS(): %d Error loading X509 key pair ", 507)+err.Error(), "FATAL")
				node.SignalChannel <- os.Interrupt
				return
			}

			node.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}

			tlsUpgrade := tls.Server(conn, node.TLSConfig)
			err = tlsUpgrade.Handshake()
			if err != nil {
				conn.Close()
				continue
			} // Upgrade client connection
			conn = net.Conn(tlsUpgrade)
		}

		conn.SetReadDeadline(time.Now().Add(time.Millisecond * 150))
		auth, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				conn.Close()
				continue // Take next connection not waiting
			}
			node.PLof("StartTCP_TLS(): "+fmt.Sprintf("StartTCPListener(): %s", err.Error()), "ERROR")
			continue
		}

		authSpl := strings.Split(strings.TrimSpace(auth), "Key:")
		if len(authSpl) != 2 {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 1, "Missing authentication header.")))
			conn.Close()
			continue
		}

		if node.Config.Key == strings.TrimSpace(authSpl[1]) {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 0, "Authentication successful.")))

			node.Wg.Add(1)
			go node.HandleClientConnection(conn)
		} else {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}
	}
}

func (node *Node) HandleClientConnection(conn net.Conn) {
	defer node.Wg.Done()
	defer conn.Close()
	text := textproto.NewConn(conn)
	defer text.Close()

	for {

		conn.SetReadDeadline(time.Now().Add(time.Nanosecond * 1000000))

		read, err := text.ReadLine()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if node.Context.Err() != nil {
					break
				}
				continue
			} else {
				break
			}
		}
		// Only another node would send SYNC DATA after passing shared node-cluster key at which point the current node will start to consume serialized data to marshal into database hashmap
		if strings.HasPrefix(read, "SYNC DATA") {
			node.PLof("HandleClientConnection(): "+fmt.Sprintf("%d Starting to sync to with master node %s", 216, conn.RemoteAddr().String()), "INFO")
			// Handle sync
			conn.Write([]byte(fmt.Sprintf("%d Node ready for sync.\r\n", 106)))
			conn.SetReadDeadline(time.Now().Add(time.Minute * time.Duration(node.Config.ReplicationSyncTimeout))) // replication-sync-timeout - as your nodes grow the time you'll have to give a node to node replica connection will have to grow, so increase accordingly.  Default is 10 minutes
			dec := gob.NewDecoder(conn)

			err = dec.Decode(&node.Data.Map)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("%d Could not decode serialized sync data into hashmap.\r\n", 108)))
				node.PLof("HandleClientConnection(): "+fmt.Sprintf("%d Could not decode serialized sync data into hashmap.\r\n", 108), "INFO")
				continue
			}

			// Setup collection mutexes
			for c, _ := range node.Data.Map {
				node.Data.Writers[c] = &sync.RWMutex{}
			}

			node.PLof("HandleClientConnection(): "+fmt.Sprintf("%d Synced up with master node %s", 217, conn.RemoteAddr().String()), "INFO")
			conn.Write([]byte(fmt.Sprintf("%d Node replica synced successfully.\r\n", 107)))
			continue
		}

		response := make(map[string]interface{}) // response back to cluster

		request := make(map[string]interface{}) // request incoming from cluster

		err = json.Unmarshal([]byte(strings.TrimSpace(string(read))), &request)
		if err != nil {
			response["statusCode"] = 4000
			response["message"] = "Unmarshalable JSON."
			r, _ := json.Marshal(response)
			text.PrintfLine(string(r))
			continue
		}

		action, ok := request["action"] // An action is insert, select, delete, ect..
		if ok {
			switch {
			case strings.EqualFold(action.(string), "collections"):
				var collections []string
				for coll, _ := range node.Data.Map {
					collections = append(collections, coll)
				}

				response["collections"] = collections
				r, _ := json.Marshal(response)
				text.PrintfLine(strings.ReplaceAll(string(r), "%", "%%"))
				continue
			case strings.EqualFold(action.(string), "delete key"):
				updatedDocs := node.DeleteKeyFromColl(request["collection"].(string), request["key"].(string))
				if updatedDocs == 0 {
					response["statusCode"] = 4022
					response["message"] = "No documents found to alter."
					response["updated"] = updatedDocs
				} else {
					response["statusCode"] = 4021
					response["message"] = "Document key removed from collection successfully."
					response["altered"] = updatedDocs
				}

				r, _ := json.Marshal(response)
				text.PrintfLine(strings.ReplaceAll(string(r), "%", "%%"))
				continue
			case strings.EqualFold(action.(string), "delete"):
				qqId := node.AddToQueryQueue(request)

				results := node.Delete(request["collection"].(string), request["keys"], request["values"], int(request["limit"].(float64)), int(request["skip"].(float64)), request["oprs"], request["lock"].(bool), request["conditions"].([]interface{}), request["sort-pos"].(string), request["sort-key"].(string))
				r, _ := json.Marshal(results)
				response["statusCode"] = 2000

				if reflect.DeepEqual(results, nil) || len(results) == 0 {
					response["message"] = "No documents deleted."
				} else {
					response["collection"] = request["collection"].(string)
					response["message"] = fmt.Sprintf("%d Document(s) deleted successfully.", len(results))
				}

				response["deleted"] = results

				r, _ = json.Marshal(response)

				if strings.Contains(response["message"].(string), "Document(s) deleted successfully.") { // Only transmit to observer if there is actual deleted docs
					go node.SendToObservers(string(r))
				}

				node.RemoveFromQueryQueue(qqId) // Query has completed remove from queue

				text.PrintfLine(strings.ReplaceAll(string(r), "%", "%%"))
				continue
			case strings.EqualFold(action.(string), "select"):

				if request["count"] == nil {
					request["count"] = false
				}

				results := node.Select(request["collection"].(string), request["keys"], request["values"], int(request["limit"].(float64)), int(request["skip"].(float64)), request["oprs"], request["lock"].(bool), request["conditions"].([]interface{}), false, request["sort-pos"].(string), request["sort-key"].(string), request["count"].(bool), false)
				r, _ := json.Marshal(results)
				text.PrintfLine(strings.ReplaceAll(string(r), "%", "%%")) // fix for (MISSING)
				continue
			case strings.EqualFold(action.(string), "update"):

				qqId := node.AddToQueryQueue(request)

				results := node.Update(request["collection"].(string),
					request["keys"], request["values"],
					int(request["limit"].(float64)), int(request["skip"].(float64)), request["oprs"],
					request["lock"].(bool),
					request["conditions"].([]interface{}),
					request["update-keys"].([]interface{}), request["new-values"].([]interface{}),
					request["sort-pos"].(string), request["sort-key"].(string))
				r, _ := json.Marshal(results)

				response["statusCode"] = 2000

				if reflect.DeepEqual(results, nil) || len(results) == 0 {
					response["message"] = "No documents updated."
				} else {
					response["collection"] = request["collection"].(string)
					response["message"] = fmt.Sprintf("%d Document(s) updated successfully.", len(results))
				}

				response["updated"] = results
				r, _ = json.Marshal(response)

				if strings.Contains(response["message"].(string), "Document(s) updated successfully.") { // Only transmit to observer if there is actual updated docs
					go node.SendToObservers(string(r))
				}

				node.RemoveFromQueryQueue(qqId) // Query has completed remove from queue

				text.PrintfLine(strings.ReplaceAll(string(r), "%", "%%"))
				continue
			case strings.EqualFold(action.(string), "insert"):

				collection := request["collection"]
				doc := request["document"]

				qqId := node.AddToQueryQueue(request)

				err = node.Insert(collection.(string), doc.(map[string]interface{}), conn)
				if err != nil {
					// Only error returned is a 4003 which means cannot insert nested object
					response["statusCode"] = strings.Split(err.Error(), " ")[0]
					response["collection"] = collection.(string)
					response["message"] = strings.Join(strings.Split(err.Error(), " ")[1:], " ")
					r, _ := json.Marshal(response)
					text.PrintfLine(string(r))
					continue
				}

				node.RemoveFromQueryQueue(qqId) // Query has completed remove from queue

				continue
			default:

				response["statusCode"] = 4002
				response["message"] = "Invalid/Non-existent action."
				r, _ := json.Marshal(response)

				text.PrintfLine(string(r))
				continue
			}
		} else {
			response["statusCode"] = 4001
			response["message"] = "Missing action." // Missing select, insert
			r, _ := json.Marshal(response)

			text.PrintfLine(string(r))
			continue
		}

	}
}
