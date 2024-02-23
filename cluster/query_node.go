package cluster

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// QueryNodes queries all nodes in parallel and gets responses
func (cluster *ClusterNode) QueryNodes(connection *Connection, body map[string]interface{}) error {

	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	wgPara := &sync.WaitGroup{}
	muPara := &sync.RWMutex{}
	for _, n := range cluster.NodeConnections {
		if !n.Replica {
			wgPara.Add(1)
			go cluster.QueryNode(n, jsonString, wgPara, muPara, &responses, body["action"].(string))
		}
	}

	wgPara.Wait()

	if cluster.Config.JoinResponses {
		if body["action"].(string) == "select" {
			var docs []interface{}

			count := 0 // if count
			isCount := false

			for _, res := range responses {

				if strings.Contains(res, "\"statusCode\": 105") {
					cluster.Printl("QueryNodes(): "+res, "INFO")
					continue
				}

				var x []interface{}
				err := json.Unmarshal([]byte(res), &x)
				if err != nil {
					return errors.New(fmt.Sprintf("%d Unmarsharable JSON.", 4018))
				}

				if len(x) > 0 {
					c, ok := x[0].(map[string]interface{})["count"]
					if ok {
						if !isCount {
							isCount = true
						}

						count += int(c.(float64))
					}
				}

				if !isCount {
					docs = append(docs, x...)
				}
			}

			if !isCount {

				if body["limit"] != -1 {
					var docsToRemoveFromResponse []int
					for i, _ := range docs {
						if i >= body["limit"].(int) {
							docsToRemoveFromResponse = append(docsToRemoveFromResponse, i)
						}
					}

					for _, i := range docsToRemoveFromResponse {
						copy(docs[i:], docs[i+1:])
						docs[len(docs)-1] = ""
						docs = docs[:len(docs)-1]
					}

					if body["sort-key"] != "" && body["sort-pos"] != "" {

						for _, d := range docs {

							doc, ok := d.(map[string]interface{})[body["sort-key"].(string)]
							if ok {
								if reflect.TypeOf(doc).Kind().String() == "string" {
									// alphabetical sorting based on string[0] value A,B,C asc C,B,A desc
									sort.Slice(docs[:], func(z, x int) bool {
										if body["sort-pos"].(string) == "asc" {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(string) < docs[x].(map[string]interface{})[body["sort-key"].(string)].(string)
										} else {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(string) > docs[x].(map[string]interface{})[body["sort-key"].(string)].(string)
										}
									})
								} else if reflect.TypeOf(d.(map[string]interface{})[body["sort-key"].(string)]).Kind().String() == "float64" {
									// numerical sorting based on float64[0] value 1.1,1.0,0.9 desc 0.9,1.0,1.1 asc
									sort.Slice(docs[:], func(z, x int) bool {
										if body["sort-pos"].(string) == "asc" {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(float64) < docs[x].(map[string]interface{})[body["sort-key"].(string)].(float64)
										} else {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(float64) > docs[x].(map[string]interface{})[body["sort-key"].(string)].(float64)
										}
									})
								} else if reflect.TypeOf(d.(map[string]interface{})[body["sort-key"].(string)]).Kind().String() == "int" {
									// numerical sorting based on int[0] value 22,12,3 desc 3,12,22 asc
									sort.Slice(docs[:], func(z, x int) bool {
										if body["sort-pos"].(string) == "asc" {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(int) < docs[x].(map[string]interface{})[body["sort-key"].(string)].(int)
										} else {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(int) > docs[x].(map[string]interface{})[body["sort-key"].(string)].(int)
										}
									})
								}

							}

						}
					}
					docsJson, err := json.Marshal(docs)
					if err != nil {
						return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
					}

					connection.Text.PrintfLine(string(docsJson))
				} else {
					if body["sort-key"] != "" && body["sort-pos"] != "" {

						for _, d := range docs {

							doc, ok := d.(map[string]interface{})[body["sort-key"].(string)]
							if ok {
								if reflect.TypeOf(doc).Kind().String() == "string" {
									// alphabetical sorting based on string[0] value A,B,C asc C,B,A desc
									sort.Slice(docs[:], func(z, x int) bool {
										if body["sort-pos"].(string) == "asc" {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(string) < docs[x].(map[string]interface{})[body["sort-key"].(string)].(string)
										} else {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(string) > docs[x].(map[string]interface{})[body["sort-key"].(string)].(string)
										}
									})
								} else if reflect.TypeOf(d.(map[string]interface{})[body["sort-key"].(string)]).Kind().String() == "float64" {
									// numerical sorting based on float64[0] value 1.1,1.0,0.9 desc 0.9,1.0,1.1 asc
									sort.Slice(docs[:], func(z, x int) bool {
										if body["sort-pos"].(string) == "asc" {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(float64) < docs[x].(map[string]interface{})[body["sort-key"].(string)].(float64)
										} else {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(float64) > docs[x].(map[string]interface{})[body["sort-key"].(string)].(float64)
										}
									})
								} else if reflect.TypeOf(d.(map[string]interface{})[body["sort-key"].(string)]).Kind().String() == "int" {
									// numerical sorting based on int[0] value 22,12,3 desc 3,12,22 asc
									sort.Slice(docs[:], func(z, x int) bool {
										if body["sort-pos"].(string) == "asc" {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(int) < docs[x].(map[string]interface{})[body["sort-key"].(string)].(int)
										} else {
											return docs[z].(map[string]interface{})[body["sort-key"].(string)].(int) > docs[x].(map[string]interface{})[body["sort-key"].(string)].(int)
										}
									})
								}

							}

						}
					}
					docsJson, err := json.Marshal(docs)
					if err != nil {
						return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
					}

					connection.Text.PrintfLine(string(docsJson))
				}
			} else {
				countResponse := make(map[string]interface{})
				countResponse["count"] = count

				countJson, err := json.Marshal(countResponse)
				if err != nil {
					return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
				}

				connection.Text.PrintfLine(string(countJson))
			}
		} else {
			var response string
			for key, res := range responses {
				response += fmt.Sprintf(`{"%s": %s},`, key, res)
			}

			connection.Text.PrintfLine(fmt.Sprintf("[%s]", strings.TrimSuffix(response, ",")))
		}

	} else {
		var response string
		for key, res := range responses {
			response += fmt.Sprintf(`{"%s": %s},`, key, res)
		}

		connection.Text.PrintfLine(fmt.Sprintf("[%s]", strings.TrimSuffix(response, ",")))

	}

	return nil
}

// QueryNodesRet queries all nodes and combines responses
func (cluster *ClusterNode) QueryNodesRet(body map[string]interface{}) map[string]string {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	wgPara := &sync.WaitGroup{}
	muPara := &sync.RWMutex{}
	for _, n := range cluster.NodeConnections {
		if !n.Replica {
			wgPara.Add(1)
			go cluster.QueryNode(n, jsonString, wgPara, muPara, &responses, body["action"].(string))
		}
	}

	wgPara.Wait()

	return responses
}

// QueryNode queries a specific node
func (cluster *ClusterNode) QueryNode(n *NodeConnection, body []byte, wg *sync.WaitGroup, mu *sync.RWMutex, responses *map[string]string, action string) {
	defer wg.Done() // defer returning go routine to waitgroup
	n.Mu.Lock()

	defer n.Mu.Unlock()

	mn := n // non replica main node

	retriesReplica := len(n.Node.Replicas) // Retry on configured node read replicas

	retriesMainNode := 3 // Retry main node 3 times

	var attemptedReplicas []string

	goto query

query:
	n.Text.Reader.R = bufio.NewReaderSize(n.Conn, cluster.Config.NodeReaderSize)

	n.Text.PrintfLine("%s", string(body))

	n.Conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cluster.Config.NodeReadDeadline))) // Timeout if node doesn't respond within NodeReadDeadline seconds(we will try 3 more times before trying a replica)
	line, err := n.Text.ReadLine()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			n.Ok = false
			if len(n.Node.Replicas) == 0 {
				retriesMainNode -= retriesMainNode

				if retriesMainNode > -1 {
					goto query
				}

			}

		} else if errors.Is(err, io.EOF) {
			n.Ok = false
			if len(n.Node.Replicas) == 0 {
				retriesMainNode -= retriesMainNode

				if retriesMainNode > 0 {
					goto query
				}
				goto unavailable
			}
		} else {
			n.Ok = false
			if len(n.Node.Replicas) == 0 {
				retriesMainNode -= retriesMainNode

				if retriesMainNode > 0 {
					goto query
				}
			}
		}

		mn.Ok = false
		goto unavailable
	}

	mu.Lock()
	(*responses)[n.Conn.RemoteAddr().String()] = line
	mu.Unlock()
	goto fin

unavailable:
	n.Ok = false

	// Will retry on nodes replicas, if n-rep1 not available, go next n-rep2, so forth until no replicas are available.

	if strings.Contains(action, "select") || strings.Contains(action, "count") {
		for _, r := range mn.Node.Replicas {
			for _, nc := range cluster.NodeConnections {
				if fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port) == fmt.Sprintf("%s:%d", r.Host, r.Port) && nc.Replica == true {

					if slices.Contains(attemptedReplicas, fmt.Sprintf("%s:%d", r.Host, r.Port)) {
						continue
					}

					n = nc
					retriesReplica -= 1

					if retriesReplica > -1 {
						attemptedReplicas = append(attemptedReplicas, fmt.Sprintf("%s:%d", r.Host, r.Port))
						goto query
					} else {
						break
					}
				}
			}
		}
	}

	if len(mn.Node.Replicas) > 0 {
		cluster.Printl(fmt.Sprintf("QueryNode(): %d Node %s and replicas %s are unavailable.", 105, mn.Conn.RemoteAddr().String(), strings.Join((func(s []string) []string {
			if len(s) < 1 {
				return s
			}

			sort.Strings(s)
			prev := 1
			for curr := 1; curr < len(s); curr++ {
				if s[curr-1] != s[curr] {
					s[prev] = s[curr]
					prev++
				}
			}

			return s[:prev]
		})(attemptedReplicas), ",")), "WARNING")
	} else {
		cluster.Printl(fmt.Sprintf(`QueryNode(): %d Node %s is unavailable.`, 105, n.Conn.RemoteAddr().String()), "WARNING")
	}

	return
fin:
	return
}
