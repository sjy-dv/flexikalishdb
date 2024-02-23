package architecture

import (
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"strings"
	"time"

	"github.com/sjy-dv/flexikalishdb/compression"
)

// AddToQueryQueue adds to query queue and returns unique query id
func (node *Node) AddToQueryQueue(req map[string]interface{}) int {
	node.QueryQueueMu.Lock()
	defer node.QueryQueueMu.Unlock()

	queryQueueEntry := make(map[string]interface{})
	queryQueueEntry["id"] = len(node.QueryQueue) + 1
	queryQueueEntry["req"] = req

	node.QueryQueue = append(node.QueryQueue, queryQueueEntry)

	return queryQueueEntry["id"].(int)
}

func (node *Node) RemoveFromQueryQueue(id int) {
	node.QueryQueueMu.Lock()
	defer node.QueryQueueMu.Unlock()
	for i, qe := range node.QueryQueue {
		if qe["id"].(int) == id {
			node.QueryQueue[i] = node.QueryQueue[len(node.QueryQueue)-1]
			node.QueryQueue[len(node.QueryQueue)-1] = nil
			node.QueryQueue = node.QueryQueue[:len(node.QueryQueue)-1]
		}
	}

}

// SyncOutQueryQueue syncs out query queue to .qqueue file which contains a list of queries that did not finish if any.  On node start up the node will finish them off.
func (node *Node) SyncOutQueryQueue() {
	defer node.Wg.Done()

	decodedKey, err := base64.StdEncoding.DecodeString(node.Config.Key)
	if err != nil {
		node.PLof(fmt.Sprintf("SyncOutQueryQueue(): %s", err.Error()), "ERROR")
		node.SignalChannel <- os.Interrupt
		return
	}

	f, err := os.OpenFile(".qqueue", os.O_TRUNC|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		node.PLof(fmt.Sprintf("SyncOutQueryQueue(): %s", err.Error()), "ERROR")
		node.SignalChannel <- os.Interrupt
		return
	}

	for {
		if node.Context.Err() != nil {
			break
		}

		f.Truncate(0)
		f.Seek(0, 0)

		var out io.Writer

		out, _ = compression.NewWriter(f, compression.StrongCompression, decodedKey)

		enc := gob.NewEncoder(out)

		enc.Encode(node.QueryQueue)

		out.(io.Closer).Close()

		time.Sleep(time.Millisecond * 70)
	}
}

// StartRunQueryQueue runs all queries that were on queue before shutdown
func (node *Node) StartRunQueryQueue() {
	qq, err := os.OpenFile(fmt.Sprintf(".qqueue"), os.O_RDONLY, 0777)
	if err != nil {
		node.PLof("StartRunQueryQueue(): "+fmt.Sprintf("%d No .qqueue file found.  Possibly first run, if so the node will create the .qqueue file after run of this method.", 120), "ERROR")
		return
	}

	var in io.Reader

	decodedKey, err := base64.StdEncoding.DecodeString(node.Config.Key)
	if err != nil {
		node.PLof("StartRunQueryQueue(): "+fmt.Sprintf("%d Could not decode configured shared key. ", 115)+err.Error(), "ERROR")
		os.Exit(1)
		return
	}

	in = compression.NewReader(qq, decodedKey)

	dec := gob.NewDecoder(in)

	defer in.(io.Closer).Close()

	err = dec.Decode(&node.QueryQueue)
	if err != nil {
		node.PLof(fmt.Sprintf("StartRunQueryQueue(): %d Node could not recover query queue.", 502), "ERROR")
		return
	}

	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", node.Config.Host, node.Config.Port)) // not catching error here, no chances an error would occur at this point

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		node.PLof(fmt.Sprintf("StartRunQueryQueue(): %d Could not dial self to requeue queries. %s", 503, err), "ERROR")
		return
	}

	text := textproto.NewConn(conn)
	text.PrintfLine("Key: %s", node.Config.Key)

	read, err := text.ReadLine()

	completed := 0 // amount of txns completed off restored queue

	if strings.HasPrefix(read, "0") {
		for _, qe := range node.QueryQueue {

			r, _ := json.Marshal(qe["req"])
			_, err = conn.Write([]byte(fmt.Sprintf("%s\r\n", string(r))))
			if err != nil {
				node.PLof(fmt.Sprintf("StartRunQueryQueue(): %d Could not commit to queued query/transaction. %s", 504, err), "ERROR")
				continue
			}

			completed += 1

			//read, _ = text.ReadLine() If you want to see what's been processed from queue

		}

		node.PLof(fmt.Sprintf("StartRunQueryQueue(): %d %d recovered and processed from .qqueue.", 505, completed), "INFO")
	}

}
