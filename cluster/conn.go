package cluster

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/textproto"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// HandleClientConnection handles tcp/tls client connection
func (cluster *ClusterNode) HandleClientConnection(conn net.Conn, user map[string]interface{}) {
	defer cluster.Wg.Done() // defer report return to waitgroup
	defer conn.Close()      // defer client connection close
	text := textproto.NewConn(conn)
	defer text.Close() // defer close connection writer and reader

	query := "" // clients current query

	for {
		conn.SetReadDeadline(time.Now().Add(time.Nanosecond * 1000000)) // essentially keep listening until the client closes connection or cluster shuts down
		read, err := text.ReadLine()                                    // read line from client
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if cluster.Context.Err() != nil { // if signaled to shutdown
					break
				}
				continue // continue on listening to client
			} else {
				break
			}
		}

		// Does line end with a semicolon?
		if strings.HasSuffix(strings.TrimSpace(string(read)), ";") {
			query += strings.TrimSpace(string(read))
		} else {
			query += strings.TrimSpace(string(read)) + " "
		}

		if strings.HasPrefix(query, "ping") { // ping
			text.PrintfLine("pong")
			query = ""
			continue
		} else if strings.HasSuffix(query, ";") { // Does line end with a semicolon?

			if cluster.Config.LogQuery { // Only log if configuration set as queries can contain harmful personal information
				cluster.Printl(fmt.Sprintf("HandleClientConnection(): %s query(%s)", conn.RemoteAddr().String(), query), "INFO")
			}

			// Check user permission and check if their allowed to use the specific action
			switch user["permission"] {
			case "R":
				if strings.HasPrefix(query, "update") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "insert") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "new user") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "users") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "delete user") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "delete key") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "delete") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed, ret
				} else if strings.HasPrefix(query, "select") {
					goto allowed // Goto allowed
				} else if strings.HasPrefix(query, "collections") {
					goto allowed // Goto allowed
				}
			case "RW":
				goto allowed // Goto allowed
			}

		continueOn: // User isn't allowed to use action but continue listening for something else
			query = ""
			continue

		allowed: // User is allowed

			switch {
			// query starts with collections
			case strings.HasPrefix(query, "collections"):
				// get all cluster collections

				// Start node request
				body := make(map[string]interface{})
				body["action"] = "collections"

				// query nodes
				err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				query = ""
				continue

			// Processing read query as CDQL
			case strings.HasPrefix(query, "insert "):
				// start insert
				// insert into users({"firstName": "John", "lastName": "Doe"});

				if len(strings.Split(query, "},")) > 1 {
					text.PrintfLine(fmt.Sprintf("%d Batch insertion not supported.", 4024))
					query = ""
					continue
				}

				retries := 5 // how many times to retry if a node is not available for $id uniqueness. Mind you we already retry nodes and replicas at this stage.
				// query is not valid
				// must have a full prefix of 'insert into '
				if !strings.HasPrefix(query, "insert into ") {
					text.PrintfLine(fmt.Sprintf("%d Invalid insert query missing 'insert into'.", 4009))
					query = "" // Clear query variable and listen for another
					continue
				}

				// Regex for insert i.e coll({}) in-between parenthesis
				var insertJsonRegex = regexp.MustCompile(`\((.*?)\)`)

				insertJson := insertJsonRegex.FindStringSubmatch(query) // Get insert JSON

				collection := strings.ReplaceAll(strings.Split(query, "({\"")[0], "insert into ", "") // Get collection

				if len(insertJson) != 2 {
					text.PrintfLine(fmt.Sprintf("%d Invalid insert query is missing parentheses.", 4010))
					query = ""
					continue
				}

				// Check if insert is json array i.e insert i.e `insert into example([{"x": 4, "y": 44}, {"x": 2, "y": 22}]);`
				// This is not allowed.  If you want to batch insert create many connections to the cluster and insert in parallel.
				if strings.HasPrefix(strings.TrimSpace(insertJson[1]), "[{") && strings.HasSuffix(strings.TrimSpace(insertJson[1]), "}]") {

					text.PrintfLine(fmt.Sprintf("%d Batch insertion not supported.", 4024))
					query = ""
					continue
				}

				// Check reserved words based on https://go.dev/ref/spec and clusterDB system reserved words

				// Check reserved keys within json keys
				for _, rk := range reservedKeys {
					if strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), rk) {
						text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 4030))
						query = ""
						goto breakOutAfterBadKey
					}
				}

				// Check reserved for reserved symbol within json keys
				for _, rs := range reservedSymbols {
					if strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), rs) {
						text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 4031))
						query = ""
						goto breakOutAfterBadKey
					}
				}

				goto keyOk

			breakOutAfterBadKey:
				continue

				goto keyOk // insert key is ok

			keyOk:

				// Checking if there are any !s to process
				var indexed = regexp.MustCompile(`"([^"]+!)"`) // "email!":
				// "key!" means check all nodes if this key and value exists
				// if an array "key!": [arr]
				// cluster will check all values within the basic array.

				indexedRes := indexed.FindAllStringSubmatch(query, -1)
				if len(indexedRes) > 0 {
					cluster.UniquenessMu.Lock()
				}

				// loop over unique key value pairs checking nodes
				// Returns error 4004 to client if a document exists
				for _, indx := range indexedRes {

					// Read json key VALUE(s)!
					kValue := regexp.MustCompile(fmt.Sprintf(`"%s"\s*:\s*(true|false|null|[A-Za-z]|\[.*?\]|[0-9]*[.]?[0-9]+|".*?"|'.*?')`, indx[1]))

					// Create node request
					body := make(map[string]interface{})
					body["action"] = "select"       // We will select 1 from all nodes with provided key value
					body["limit"] = 1               // limit of 1 of course
					body["collection"] = collection // collection is provided collection
					body["conditions"] = []string{""}
					body["skip"] = 0
					body["sort-pos"] = ""
					body["sort-key"] = ""
					var interface1 []interface{} // In-order to have an interface slice in go you must set them up prior to using them.
					var interface2 []interface{} // ^
					var interface3 []interface{} // ^

					body["keys"] = interface1   // We send nodes an array of keys to query
					body["oprs"] = interface2   // We send nodes an array of oprs to use for query
					body["values"] = interface3 // Values for query
					// There must be equal keys, oprs, and values.

					body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
					body["oprs"] = append(body["oprs"].([]interface{}), "==")                                                // == obviously

					body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup uniqueness across all nodes if indexes are required

					if len(kValue.FindStringSubmatch(query)) > 0 {
						if strings.HasPrefix(kValue.FindStringSubmatch(query)[1], "[") && strings.HasSuffix(kValue.FindStringSubmatch(query)[1], "]") {
							var arr []interface{}
							err := json.Unmarshal([]byte(kValue.FindStringSubmatch(query)[1]), &arr)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unmarsharable JSON insert.", 4000))
								query = ""
								goto cont
							}

							for j, a := range arr {

								body["values"] = append(body["values"].([]interface{}), a)
								if j == 0 {
									body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
									body["oprs"] = append(body["oprs"].([]interface{}), "==")
								}
							}

							res := cluster.QueryNodesRet(body)
							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists.")

									r, _ := json.Marshal(result)
									text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}

						} else {

							body["values"] = append(body["values"].([]interface{}), kValue.FindStringSubmatch(query)[1])

							if strings.EqualFold(body["values"].([]interface{})[0].(string), "null") {
								body["values"].([]interface{})[0] = nil
							} else if cluster.IsString(body["values"].([]interface{})[0].(string)) {

								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "'")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "'")
							} else if cluster.IsInt(body["values"].([]interface{})[0].(string)) {
								i, err := strconv.Atoi(body["values"].([]interface{})[0].(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
									query = ""
									goto cont
								}

								body["values"].([]interface{})[0] = i

							} else if cluster.IsFloat(body["values"].([]interface{})[0].(string)) {

								f, err := strconv.ParseFloat(body["values"].([]interface{})[0].(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
									query = ""
									goto cont
								}

								body["values"].([]interface{})[0] = f
							} else if cluster.IsBool(body["values"].([]interface{})[0].(string)) {

								b, err := strconv.ParseBool(body["values"].([]interface{})[0].(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
									query = ""
									goto cont
								}

								body["values"].([]interface{})[0] = b
							}

							res := cluster.QueryNodesRet(body)

							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists.")

									r, _ := json.Marshal(result)
									text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}
						}
					}
				}

				goto ok

			cont:
				if len(indexedRes) > 0 {
					cluster.UniquenessMu.Unlock()
				}
				query = ""
				continue

			ok:
				body := make(map[string]interface{})

				var interface1 []interface{}
				var interface2 []interface{}
				var interface3 []interface{}
				body["action"] = "select"
				body["limit"] = 1
				body["skip"] = 0
				body["collection"] = collection
				body["conditions"] = []string{""}
				body["sort-pos"] = ""
				body["sort-key"] = ""

				body["keys"] = interface1
				body["keys"] = append(body["keys"].([]interface{}), "$id")
				body["oprs"] = interface2
				body["oprs"] = append(body["oprs"].([]interface{}), "==")

				body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup uniqueness across all nodes
				body["values"] = interface3
				body["values"] = append(body["values"].([]interface{}), uuid.New().String())

				res := cluster.QueryNodesRet(body)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						if retries != 0 {
							retries -= 1
							goto retry // $id already exist
						} else {
							text.PrintfLine(fmt.Sprintf("%d No unique $id could be found for insert.", 4023)) // Wouldn't happen ever but if it does the system as you can see would try at least 5 times
							goto cont
						}
					}
				}

				goto insert
			retry:
				body["values"].([]interface{})[0] = uuid.New().String() // Generate new uuid

				res = cluster.QueryNodesRet(body)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						if retries != 0 {
							retries -= 1
							goto retry // $id already exist
						} else {
							text.PrintfLine(fmt.Sprintf("%d No unique $id could be found for insert.", 4023)) // Wouldn't happen ever but if it does the system as you can see would try at least 5 times
							goto cont
						}
					}
				}

				goto insert

			insert:
				cluster.InsertIntoNode(&Connection{Conn: conn, Text: text, User: nil}, strings.ReplaceAll(insertJson[1], "!\":", "\":"), collection, body["values"].([]interface{})[0].(string))
				if len(indexedRes) > 0 {
					cluster.UniquenessMu.Unlock()
				}
				query = ""
				continue
				// end insert
			case strings.HasPrefix(query, "select "):
				// start select
				// select LIMIT from COLLECTION SET KEY = V SET KEY = V;
				// update LIMIT from COLLECTION where KEY = V && KEY = V order by KEY desc;

				if !strings.Contains(query, "from ") {
					text.PrintfLine(fmt.Sprintf("%d From is required.", 4006))
					query = ""
					continue
				}

				if strings.Contains(query, "not like") {
					query = strings.ReplaceAll(query, "not like", "!like")
				}

				sortPos := ""
				sortKey := ""

				if strings.Contains(query, "order by ") {
					sortKey = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(query[strings.Index(query, "order by "):], "order by "), "asc;"), "desc;"))
					if strings.HasSuffix(query, "asc;") {
						sortPos = "asc"
					} else {
						sortPos = "desc"
					}

					query = query[:strings.Index(query, "order by ")]
				}

				qsreg := regexp.MustCompile("'.+'|\".+\"|\\S+")

				querySplit := qsreg.FindAllString(strings.Replace(strings.Replace(query, "from", "", 1), "where", "", 1), -1)

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]

					if len(querySplit) == 2 {
						text.PrintfLine(fmt.Sprintf("%d Missing limit value.", 4016))
						query = ""
						continue
					}

					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["skip"] = 0
					body["conditions"] = []string{""}
					body["lock"] = false // lock on read.  There can be many clusters reading at one time.
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					if body["limit"].(string) != "count" {

						if body["limit"].(string) == "*" {
							body["limit"] = -1
						} else if strings.Contains(body["limit"].(string), ",") {
							if len(strings.Split(body["limit"].(string), ",")) == 2 {
								body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
									query = ""
									continue
								}

								if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
									body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
									if err != nil {
										text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 4028, err.Error()))
										query = ""
										continue
									}
								} else {
									body["limit"] = -1
								}
							} else {
								text.PrintfLine("%d Invalid limiting value.", 4029)
								query = ""
								continue
							}
						} else {
							body["limit"], err = strconv.Atoi(body["limit"].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
								query = ""
								continue
							}
						}
					} else {
						body["limit"] = -2
						body["count"] = true
					}

					if strings.Contains(query, "where") {
						if len(body["values"].([]interface{})) == 0 || body["values"] == nil {
							text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 4025))
							query = ""
							continue
						}
					}

					err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				} else {
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["skip"] = 0
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					for k, s := range andOrSplit {
						querySplitNested := qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.Replace(strings.TrimSuffix(s, ";"), "from", "", 1), "where", "", 1)), -1)
						if len(querySplitNested) < 3 {
							text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
							query = ""
							goto extCont
						}

						body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
						body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])

						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "="):
							body["oprs"].([]interface{})[k] = "=="
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "like"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!like"):
						default:
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
							query = ""
							goto extCont
						}

						body["values"] = append(body["values"].([]interface{}), strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"))

						lindx := strings.Index(query, fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						if len(query[lindx+valLen:]) > 3 {
							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(query[lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cluster.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cluster.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						} else if cluster.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cluster.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						}

					}

					if len(body["values"].([]interface{})) == 0 {
						text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 4025))
						query = ""
						continue
					}

					if body["limit"].(string) != "count" {
						if body["limit"].(string) == "*" {
							body["limit"] = -1
						} else if strings.Contains(body["limit"].(string), ",") {
							if len(strings.Split(body["limit"].(string), ",")) == 2 {
								var err error
								body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
									query = ""
									continue
								}

								if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
									body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
									if err != nil {
										text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
										query = ""
										continue
									}
								} else {
									body["limit"] = -1
								}
							} else {
								text.PrintfLine("%d Invalid limiting value.", 4029)
								query = ""
								continue
							}
						} else {
							var err error
							body["limit"], err = strconv.Atoi(body["limit"].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
								query = ""
								continue
							}
						}
					} else {
						body["limit"] = -2
						body["count"] = true

					}

					err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				extCont:
					continue

				}
				// end select
			case strings.HasPrefix(query, "update "):
				// start update
				// update LIMIT in COLLECTION SET KEY = V SET KEY = V;
				// update LIMIT in COLLECTION where KEY = V && KEY = V order by KEY desc SET KEY = V;

				if !strings.Contains(query, "in ") {
					text.PrintfLine(fmt.Sprintf("%d In is required.", 4020))
					query = ""
					continue
				}

				query = strings.ReplaceAll(query, "not like", "!like")

				sortPos := ""
				sortKey := ""

				if strings.Contains(query, "order by ") {
					sortKey = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(query[strings.Index(query, "order by "):], "order by "), "asc;"), "desc;"))
					if strings.HasSuffix(query, "asc;") {
						sortPos = "asc"
					} else {
						sortPos = "desc"
					}

					query = query[:strings.Index(query, "order by ")]
				}

				qsreg := regexp.MustCompile("'.+'|\".+\"|\\S+")
				querySplit := qsreg.FindAllString(strings.Replace(strings.Replace(strings.Replace(query, "in", "", 1), "from", "", 1), "where", "", 1), -1)

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]

					if len(querySplit) == 2 {
						text.PrintfLine(fmt.Sprintf("%d Missing limit value.", 4016))
						query = ""
						continue
					}

					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}
					var interface4 []interface{}
					var interface5 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["update-keys"] = interface4
					body["new-values"] = interface5
					body["conditions"] = []string{""}
					body["lock"] = false
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey
					body["skip"] = 0

					// Get new key values
					if len(strings.Split(query, "set ")) == 1 {
						text.PrintfLine(fmt.Sprintf("%d Update sets are missing.", 4019))
						query = ""
						continue
					}

					for _, s := range strings.Split(query, "set ")[1:] {
						newValues := []string{strings.ReplaceAll(s, "set ", "")}

						for _, nvSet := range newValues {
							spl := strings.Split(nvSet, " = ")
							body["update-keys"] = append(body["update-keys"].([]interface{}), strings.TrimSpace(spl[0]))
							var val interface{}
							if len(spl) != 2 {
								text.PrintfLine(fmt.Sprintf("%d Set is missing =.", 4008))
								query = ""
								goto extCont5
							}

							val = strings.TrimSuffix(strings.TrimSpace(spl[1]), ";")
							if strings.EqualFold(val.(string), "null") {
								val = nil
							} else if cluster.isArray(val.(string)) {
								var jsonArrSlice []interface{}
								err = json.Unmarshal([]byte(val.(string)), &jsonArrSlice)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Invalid set array values. %s", 4032, err))
									query = ""
									goto extCont5
								}

								val = jsonArrSlice
							} else if cluster.IsString(val.(string)) {

								val = strings.TrimSuffix(val.(string), "\"")
								val = strings.TrimPrefix(val.(string), "\"")
								val = strings.TrimSuffix(val.(string), "'")
								val = strings.TrimPrefix(val.(string), "'")
							} else if cluster.IsInt(val.(string)) {
								i, err := strconv.Atoi(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
									query = ""
									goto extCont5
								}

								val = i

							} else if cluster.IsFloat(val.(string)) {

								f, err := strconv.ParseFloat(val.(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
									query = ""
									goto extCont5
								}

								val = f
							} else if cluster.IsBool(val.(string)) {

								b, err := strconv.ParseBool(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
									query = ""
									goto extCont5
								}

								val = b
							}
							body["new-values"] = append(body["new-values"].([]interface{}), val)
						}
					}

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 4028, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine(fmt.Sprintf("%d Invalid limiting value.", 4029))
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 4028, err.Error()))
							query = ""
							continue
						}
					}

					err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				extCont5:
					query = ""
					continue

				} else { // With conditions
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["skip"] = 0
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}
					var interface4 []interface{}
					var interface5 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["update-keys"] = interface4
					body["new-values"] = interface5
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					// Get new key values
					if len(strings.Split(query, "set ")) == 1 {
						text.PrintfLine(fmt.Sprintf("%d Update sets are missing.", 4019))
						query = ""
						continue
					}

					for _, s := range strings.Split(query, "set ")[1:] {

						newValues := []string{strings.ReplaceAll(s, "set ", "")}

						for _, nvSet := range newValues {
							spl := strings.Split(nvSet, " = ")
							body["update-keys"] = append(body["update-keys"].([]interface{}), strings.TrimSpace(spl[0]))
							var val interface{}

							if len(spl) != 2 {
								text.PrintfLine(fmt.Sprintf("%d Set is missing =.", 4008))
								query = ""
								goto extCont3
							}

							val = strings.TrimSuffix(strings.TrimSpace(spl[1]), ";")
							if strings.EqualFold(val.(string), "null") {
								val = nil
							} else if cluster.isArray(val.(string)) {
								var jsonArrSlice []interface{}
								err = json.Unmarshal([]byte(val.(string)), &jsonArrSlice)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Invalid set array values. %s", 4032, err))
									query = ""
									goto extCont3
								}

								val = jsonArrSlice
							} else if cluster.IsString(val.(string)) {

								val = strings.TrimSuffix(val.(string), "\"")
								val = strings.TrimPrefix(val.(string), "\"")
								val = strings.TrimSuffix(val.(string), "'")
								val = strings.TrimPrefix(val.(string), "'")
							} else if cluster.IsInt(val.(string)) {
								i, err := strconv.Atoi(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
									query = ""
									goto extCont3
								}

								val = i

							} else if cluster.IsFloat(val.(string)) {

								f, err := strconv.ParseFloat(val.(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
									query = ""
									goto extCont3
								}

								val = f
							} else if cluster.IsBool(val.(string)) {

								b, err := strconv.ParseBool(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
									query = ""
									goto extCont3
								}

								val = b
							}
							body["new-values"] = append(body["new-values"].([]interface{}), val)
						}
					}

					for k, s := range andOrSplit {
						var querySplitNested []string
						if strings.Contains(s, "set") {
							if strings.Count(s, "where") > 1 {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.TrimSuffix(s[:strings.Index(s, "set")], ";"), "where", "", 1)), -1)

							} else {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.TrimSuffix(s[:strings.Index(s, "set")], ";")), -1)
							}

							if len(querySplitNested) < 3 {
								text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
								query = ""
								continue
							}

						} else {
							if strings.Count(s, "where") > 1 {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.TrimSuffix(s, ";"), "where", "", 1)), -1)
								if len(querySplitNested) < 3 {
									text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
									query = ""
									continue
								}
							} else {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.TrimSuffix(s, ";")), -1)
								if len(querySplitNested) < 3 {
									text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
									query = ""
									continue
								}
							}
						}

						body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(querySplitNested[len(querySplitNested)-3]))
						body["oprs"] = append(body["oprs"].([]interface{}), strings.TrimSpace(querySplitNested[len(querySplitNested)-2]))
						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "="):
							body["oprs"].([]interface{})[k] = "=="
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "like"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!like"):
						default:
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
							query = ""
							goto extCont3
						}

						body["values"] = append(body["values"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";")))

						lindx := strings.Index(strings.TrimRight(strings.Split(query, "set")[0], "set "), fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						if len(strings.Split(query, "set")[0][lindx+valLen:]) > 2 {
							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(strings.Split(query, "set")[0][lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cluster.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cluster.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						} else if cluster.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cluster.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						}

					}

					if len(body["values"].([]interface{})) == 0 {
						text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 4025))
						query = ""
						continue
					}

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine("%d Invalid limiting value.", 4029)
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
							query = ""
							continue
						}
					}

					err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue
				extCont3:
					continue
				}
				// end update
			case strings.HasPrefix(query, "delete user"):
				// start delete user
				// delete user USERNAME
				splQ := strings.Split(query, "delete user ")

				if len(splQ) != 2 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				err = cluster.RemoveUser(splQ[1])
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				text.PrintfLine("%d Database user %s removed successfully.", 201, strings.TrimSuffix(splQ[1], ";"))

				query = ""
				continue
				// end delete user
			case strings.HasPrefix(query, "delete key "):
				// start delete key
				// delete key KEY in COLLECTION;
				// removes key from all documents within a collection

				body := make(map[string]interface{})
				body["action"] = "delete key"

				if !strings.Contains(query, "in") {
					text.PrintfLine(fmt.Sprintf("%d delete key missing in.", 4026))
					query = ""
					continue
				}

				querySplit := strings.Split(strings.TrimPrefix(query, "delete key "), "in")

				if len(querySplit) < 2 {
					text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
					query = ""
					continue
				}

				body["key"] = strings.TrimSpace(querySplit[0])
				body["collection"] = strings.TrimSpace(strings.TrimSuffix(querySplit[1], ";"))

				err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				query = ""
				continue
				// end delete key
			case strings.HasPrefix(query, "delete "):
				// start delete
				// delete 1 from users where name == 'alex' && last == 'padula';

				// Check if query contains from
				if !strings.Contains(query, "from ") {
					text.PrintfLine(fmt.Sprintf("%d From is required.", 4006))
					query = ""
					continue
				}

				// not like will break parsing so we switch to !like and parse that from here alongside all the way to node
				query = strings.ReplaceAll(query, "not like", "!like")

				sortPos := "" // can either be empty or desc,asc
				sortKey := "" // sorting key like createdOn

				// We check if query contains an order by and set the sortPos with either asc or desc
				if strings.Contains(query, "order by ") {
					sortKey = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(query[strings.Index(query, "order by "):], "order by "), "asc;"), "desc;"))
					if strings.HasSuffix(query, "asc;") {
						sortPos = "asc"
					} else {
						sortPos = "desc"
					}

					query = query[:strings.Index(query, "order by ")]
				}

				qsreg := regexp.MustCompile("'.+'|\".+\"|\\S+")

				querySplit := qsreg.FindAllString(strings.Replace(strings.Replace(query, "from", "", 1), "where", "", 1), -1)

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]

					if len(querySplit) == 2 {
						text.PrintfLine(fmt.Sprintf("%d Missing limit value.", 4016))
						query = ""
						continue
					}

					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["conditions"] = []string{""}
					body["lock"] = false
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey
					body["skip"] = 0

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 4028, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine(fmt.Sprintf("%d Invalid limiting value.", 4029))
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 4028, err.Error()))
							query = ""
							continue
						}
					}

					err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				} else {
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["skip"] = 0
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					for k, s := range andOrSplit {
						querySplitNested := qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.Replace(strings.TrimSuffix(s, ";"), "from", "", 1), "where", "", 1)), -1)

						if len(querySplitNested) < 3 {
							text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
							query = ""
							goto extCont4
						}

						body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
						body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])
						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "="):
							body["oprs"].([]interface{})[k] = "=="
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "like"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!like"):
						default:
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
							query = ""
							goto extCont4
						}

						body["values"] = append(body["values"].([]interface{}), strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"))

						lindx := strings.Index(query, fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						if len(query[lindx+valLen:]) > 3 {
							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(query[lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cluster.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cluster.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						} else if cluster.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cluster.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						}

					}

					if len(body["values"].([]interface{})) == 0 {
						text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 4025))
						query = ""
						continue
					}

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine("%d Invalid limiting value.", 4029)
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 4027, err.Error()))
							query = ""
							continue
						}
					}

					err = cluster.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue
				extCont4:
					continue
				}
				// end delete
			case strings.HasPrefix(query, "new user "):
				// start new user
				// new user username, password, RW
				splitQuery := strings.Split(query, "new user ")

				// now we split at comma if value is equal to 2
				if len(splitQuery) != 2 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				// A database password CANNOT contain commas.  We do a check here to check if there's more or less than a split of 3
				//new user username, password, RW
				splQComma := strings.Split(splitQuery[1], ",")

				if len(splQComma) != 3 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				_, _, err = cluster.NewUser(strings.TrimSpace(splQComma[0]), strings.TrimSpace(splQComma[1]), strings.TrimSpace(splQComma[2]))
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				text.PrintfLine(fmt.Sprintf("%d New database user %s created successfully.", 200, strings.TrimSpace(splQComma[0])))
				query = ""
				continue
				// end new user
			case strings.HasPrefix(query, "users"):
				// start users
				var users []string

				// Get users based on in memory config
				for _, u := range cluster.Config.Users {
					username, err := base64.StdEncoding.DecodeString(strings.Split(u, ":")[0]) // get username splitting at :
					if err != nil {
						cluster.Printl("HandleClientConnection(): "+fmt.Sprintf("%d Could not decode user username.", 202), "ERROR")
						continue
					}
					users = append(users, string(username))
				}

				usersJsonArr, err := json.Marshal(users)
				if err != nil {
					text.PrintfLine("%d Could not marshal users list array.", 203)
					query = ""
					continue
				}

				text.PrintfLine(string(usersJsonArr)) // returns ["john", "jane", "jill"]

				query = ""
				continue
				// end users
			default:
				// start invalid
				text.PrintfLine("%d Invalid command/query.", 4005)
				query = ""
				continue
				// end invalid
			}

		}
	}
}

func (cluster *ClusterNode) LostReconnect() {
	defer cluster.Wg.Done() // Defer to return to waitgroup

	for {
		if cluster.Context.Err() != nil { // On signal break out of for loop
			break
		}

		for i, nc := range cluster.NodeConnections { // Iterate over node connections
			if !nc.Ok { // Check if node connection is not ok
				if cluster.Config.TLSNode { // Is TLS node configured?

					// Resolve TCP addr
					tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port))
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

					// We will keep the node connection alive until shutdown
					conn.SetKeepAlive(true) // forever

					// Configure TLS
					config := tls.Config{ServerName: nc.Node.Host} // Either ServerName or InsecureSkipVerify will do it

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

						// Check if node connection is a replica or not
						if !nc.Replica {
							cluster.NodeConnections[i] = &NodeConnection{
								Conn:       conn,
								SecureConn: secureConn,
								Text:       textproto.NewConn(secureConn),
								Node:       nc.Node,
								Mu:         &sync.Mutex{},
								Ok:         true,
							}
						} else {
							cluster.NodeConnections[i] = &NodeConnection{
								Conn:       conn,
								SecureConn: secureConn,
								Text:       textproto.NewConn(secureConn),
								Node:       nc.Node,
								Mu:         &sync.Mutex{},
								Ok:         true,
								Replica:    true, // Mark as node read replica
							}
						}

						cluster.Printl(fmt.Sprintf("LostReconnect(): %d Reconnected to lost connection ", 116)+fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port), "INFO")
						time.Sleep(time.Nanosecond * 1000000)

					}
				} else {

					// Resolve TCP addr
					tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port))
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

						// Check if node connection is a replica or not
						if !nc.Replica {
							cluster.NodeConnections[i] = &NodeConnection{
								Conn:    conn,
								Text:    textproto.NewConn(conn),
								Node:    nc.Node,
								Mu:      &sync.Mutex{},
								Ok:      true,
								Replica: false,
							}
						} else {
							cluster.NodeConnections[i] = &NodeConnection{
								Conn:    conn,
								Text:    textproto.NewConn(conn),
								Node:    nc.Node,
								Mu:      &sync.Mutex{},
								Ok:      true,
								Replica: true, // Mark as replica
							}
						}

						cluster.Printl(fmt.Sprintf("LostReconnect(): %d Reconnected to lost connection ", 116)+fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port), "INFO")
						time.Sleep(time.Nanosecond * 1000000)
					}

					time.Sleep(time.Nanosecond * 1000000)

				}
			}
		}
		time.Sleep(time.Nanosecond * 1000000)
	}

}
