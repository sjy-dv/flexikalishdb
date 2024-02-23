package architecture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
)

func (node *Node) Insert(collection string, jsonMap map[string]interface{}, conn net.Conn) error {
	if node.CurrentMemoryUsage() >= node.Config.MaxMemory {
		return errors.New(fmt.Sprintf("%d node is at peak allocation.", 100))
	}

	jsonStr, err := json.Marshal(jsonMap)
	if err != nil {
		return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
	}

	if strings.Contains(string(jsonStr), "[{\"") {
		return errors.New(fmt.Sprintf("%d Nested JSON objects not permitted.", 4003))
	} else if strings.Contains(string(jsonStr), ": {\"") {
		return errors.New(fmt.Sprintf("%d Nested JSON objects not permitted.", 4003))
	} else if strings.Contains(string(jsonStr), ":{\"") {
		return errors.New(fmt.Sprintf("%d Nested JSON objects not permitted.", 4003))
	}

	jsonStr = []byte(strings.ReplaceAll(string(jsonStr), "%", "%%")) // Because we have pattern matching logic this is a conflict so we replace % with ۞ and make ۞ a reserved value in with the entire object.

	doc := make(map[string]interface{})
	err = json.Unmarshal(jsonStr, &doc)

	if err != nil {
		return errors.New(fmt.Sprintf("%d Unmarsharable JSON insert.", 4000))
	}
	writeMu, ok := node.Data.Writers[collection]
	if ok {
		writeMu.Lock()

		node.Data.Map[collection] = append(node.Data.Map[collection], doc)

		writeMu.Unlock()
	} else {
		node.Data.Writers[collection] = &sync.RWMutex{}
		node.Data.Map[collection] = append(node.Data.Map[collection], doc)
	}

	response := make(map[string]interface{})
	response["statusCode"] = 2000
	response["message"] = "Document inserted successfully."
	response["collection"] = collection
	response["insert"] = doc

	responseJson, err := json.Marshal(response)
	if err != nil {
		return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
	}

	go node.SendToObservers(string(responseJson))
	conn.Write([]byte(string(responseJson) + "\r\n")) // Using write instead of Printf fixes issues with values that contain percentage symbols %

	return nil
}

// DeleteKeyFromColl Deletes key from all collection documents
func (node *Node) DeleteKeyFromColl(collection string, key string) int {
	var objects int
	l, ok := node.Data.Writers[collection]
	if ok {
		l.Lock()
		defer l.Unlock()
	}

	for _, d := range node.Data.Map[collection] {
		_, ok = d[key]
		if ok {
			delete(d, key)
			objects += 1
		}
	}

	return objects
}

// Search checks if provided index within data collection meets conditions
func (node *Node) Search(mu *sync.RWMutex, i int, tbd *[]int, collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, conditions []interface{}, del bool, update bool, objs *[]interface{}) {

	conditionsMetDocument := 0 // conditions met as in th first condition would be key == v lets say the next would be && or || etc..

	// if keys, values and operators are nil
	// This could be a case of "select * from users;" for example if passing skip and volume checks
	if ks == nil && vs == nil && oprs == nil {

		// add document to objects
		if update {
			node.Data.Map[collection][i]["$indx"] = i
		}

		mu.Lock()
		if len(*objs) == vol {
			mu.Unlock()
			return
		}

		*objs = append(*objs, node.Data.Map[collection][i])
		if del {
			*tbd = append(*tbd, i)
		}
		mu.Unlock()

		return
	} else {

		// range over provided keys
		for m, k := range ks.([]interface{}) {

			if oprs.([]interface{})[m] == "" {
				return
			}

			vType := fmt.Sprintf("%T", vs.([]interface{})[m])

			_, ok := node.Data.Map[collection][i][k.(string)]
			if ok {

				if node.Data.Map[collection][i][k.(string)] == nil {
					if oprs.([]interface{})[m] == "==" {
						if reflect.DeepEqual(vs.([]interface{})[m], nil) {
							conditionsMetDocument += 1
						}
					}

					continue
				}

				if reflect.TypeOf(node.Data.Map[collection][i][k.(string)]).Kind() == reflect.Slice {
					for _, dd := range node.Data.Map[collection][i][k.(string)].([]interface{}) {

						if reflect.TypeOf(dd).Kind() == reflect.Float64 {
							if vType == "int" {
								var interfaceI int = int(dd.(float64))

								if oprs.([]interface{})[m] == "==" {
									if reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}
								} else if oprs.([]interface{})[m] == "!=" {
									if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()
									}
								} else if oprs.([]interface{})[m] == ">" {
									if vType == "int" {
										if interfaceI > vs.([]interface{})[m].(int) {

											(func() {
												for _, o := range *objs {
													if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									}
								} else if oprs.([]interface{})[m] == "<" {
									if vType == "int" {
										if interfaceI < vs.([]interface{})[m].(int) {

											(func() {
												for _, o := range *objs {
													if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									}
								} else if oprs.([]interface{})[m] == ">=" {
									if vType == "int" {
										if interfaceI >= vs.([]interface{})[m].(int) {

											(func() {
												for _, o := range *objs {
													if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									}
								} else if oprs.([]interface{})[m] == "<=" {
									if vType == "int" {
										if interfaceI <= vs.([]interface{})[m].(int) {

											(func() {
												for _, o := range *objs {
													if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									}
								}
							} else if vType == "float64" {
								var interfaceI float64 = dd.(float64)

								if oprs.([]interface{})[m] == "==" {

									if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}
								} else if oprs.([]interface{})[m] == "!=" {
									if float64(interfaceI) != vs.([]interface{})[m].(float64) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}
								} else if oprs.([]interface{})[m] == ">" {
									if float64(interfaceI) > vs.([]interface{})[m].(float64) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}

								} else if oprs.([]interface{})[m] == "<" {
									if float64(interfaceI) < vs.([]interface{})[m].(float64) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}

								} else if oprs.([]interface{})[m] == ">=" {

									if float64(interfaceI) >= vs.([]interface{})[m].(float64) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}

								} else if oprs.([]interface{})[m] == "<=" {
									if float64(interfaceI) <= vs.([]interface{})[m].(float64) {

										(func() {
											for _, o := range *objs {
												if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}

								}
							}
						} else if reflect.TypeOf(dd).Kind() == reflect.Map {
							//for kkk, ddd := range dd.(map[string]interface{}) {
							//	// unimplemented
							//}
						} else {
							// string
							if oprs.([]interface{})[m] == "like" {
								vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
								vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
								vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "(MISSING)", "")
								vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
								vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
								vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
								vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

								if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
									// Get index of % and check if on left or right of string
									percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
									sMiddle := len(vs.([]interface{})[m].(string)) / 2
									right := sMiddle <= percIndex

									if right {
										r := regexp.MustCompile(`^(.*?)%`)
										patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

										for j := range patterns {
											patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
										}

										for _, p := range patterns {
											// does value start with p

											if strings.HasPrefix(dd.(string), p) {
												if skip != 0 {
													skip = skip - 1
													goto s
												}
												conditionsMetDocument += 1

											s:
												continue
											}
										}
									} else {
										r := regexp.MustCompile(`\%(.*)`)
										patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

										for j := range patterns {
											patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
										}

										for _, p := range patterns {
											// does value end with p
											if strings.HasSuffix(dd.(string), p) {
												if skip != 0 {
													skip = skip - 1
													goto s2
												}
												conditionsMetDocument += 1

											s2:
												continue
											}
										}
									}
								} else {

									r := regexp.MustCompile(`%(.*?)%`)
									patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

									for j := range patterns {
										patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
									}

									for _, p := range patterns {
										// does value contain p
										if strings.Count(dd.(string), p) > 0 {
											if skip != 0 {
												skip = skip - 1
												goto s3
											}
											conditionsMetDocument += 1

										s3:
											continue
										}
									}
								}
							} else if oprs.([]interface{})[m] == "!like" {
								vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
								vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
								vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
								vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
								vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
								vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

								// select * from users where firstName not like 'alex%'
								if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
									// Get index of % and check if on left or right of string
									percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
									sMiddle := len(vs.([]interface{})[m].(string)) / 2
									right := sMiddle <= percIndex

									if right {
										r := regexp.MustCompile(`^(.*?)%`)
										patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

										for j := range patterns {
											patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
										}

										for _, p := range patterns {
											// does value start with p
											if !strings.HasPrefix(dd.(string), p) {
												if skip != 0 {
													skip = skip - 1
													goto s4
												}
												conditionsMetDocument += 1

											s4:
												continue
											}
										}
									} else {
										r := regexp.MustCompile(`\%(.*)`)
										patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

										for j := range patterns {
											patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
										}

										for _, p := range patterns {
											// does value end with p
											if !strings.HasSuffix(dd.(string), p) {
												if skip != 0 {
													skip = skip - 1
													goto s5
												}
												conditionsMetDocument += 1

											s5:
												continue
											}
										}
									}
								} else {

									r := regexp.MustCompile(`%(.*?)%`)
									patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

									for j := range patterns {
										patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
									}

									for _, p := range patterns {
										// does value contain p
										if strings.Count(dd.(string), p) == 0 {
											if skip != 0 {
												skip = skip - 1
												goto s6
											}
											conditionsMetDocument += 1

										s6:
											continue
										}
									}
								}
							} else if oprs.([]interface{})[m] == "==" {
								if reflect.DeepEqual(dd, vs.([]interface{})[m]) {

									(func() {
										for _, o := range *objs {
											if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
												goto exists
											}
										}
										if skip != 0 {
											skip = skip - 1
											goto exists
										}
										conditionsMetDocument += 1
									exists:
									})()

								}
							} else if oprs.([]interface{})[m] == "!=" {
								if !reflect.DeepEqual(dd, vs.([]interface{})[m]) {

									(func() {
										for _, o := range *objs {
											if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
												goto exists
											}
										}
										if skip != 0 {
											skip = skip - 1
											goto exists
										}
										conditionsMetDocument += 1
									exists:
									})()

								}
							}
						}

					}
				} else if vType == "int" {
					var interfaceI int = int(node.Data.Map[collection][i][k.(string)].(float64))

					if oprs.([]interface{})[m] == "==" {
						if reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}
					} else if oprs.([]interface{})[m] == "!=" {
						if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}
					} else if oprs.([]interface{})[m] == ">" {
						if vType == "int" {
							if interfaceI > vs.([]interface{})[m].(int) {

								(func() {
									for _, o := range *objs {
										if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
											goto exists
										}
									}
									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						}
					} else if oprs.([]interface{})[m] == "<" {
						if vType == "int" {
							if interfaceI < vs.([]interface{})[m].(int) {

								(func() {
									for _, o := range *objs {
										if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
											goto exists
										}
									}
									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						}
					} else if oprs.([]interface{})[m] == ">=" {
						if vType == "int" {
							if interfaceI >= vs.([]interface{})[m].(int) {

								(func() {
									for _, o := range *objs {
										if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
											goto exists
										}
									}
									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						}
					} else if oprs.([]interface{})[m] == "<=" {
						if vType == "int" {
							if interfaceI <= vs.([]interface{})[m].(int) {

								(func() {
									for _, o := range *objs {
										if reflect.DeepEqual(o, node.Data.Map[collection][i]) {
											goto exists
										}
									}
									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						}
					}
				} else if vType == "float64" {
					var interfaceI float64 = node.Data.Map[collection][i][k.(string)].(float64)

					if oprs.([]interface{})[m] == "==" {

						if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}
					} else if oprs.([]interface{})[m] == "!=" {
						if float64(interfaceI) != vs.([]interface{})[m].(float64) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}
					} else if oprs.([]interface{})[m] == ">" {
						if float64(interfaceI) > vs.([]interface{})[m].(float64) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}

					} else if oprs.([]interface{})[m] == "<" {
						if float64(interfaceI) < vs.([]interface{})[m].(float64) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}

					} else if oprs.([]interface{})[m] == ">=" {

						if float64(interfaceI) >= vs.([]interface{})[m].(float64) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}

					} else if oprs.([]interface{})[m] == "<=" {
						if float64(interfaceI) <= vs.([]interface{})[m].(float64) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}

					}
				} else { // string

					if oprs.([]interface{})[m] == "like" {
						vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
						vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
						vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
						vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
						vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
						vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

						// select * from users where firstName like 'alex%'
						if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
							// Get index of % and check if on left or right of string
							percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
							sMiddle := len(vs.([]interface{})[m].(string)) / 2
							right := sMiddle <= percIndex

							if right {
								r := regexp.MustCompile(`^(.*?)%`)
								patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

								for j := range patterns {
									patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
								}

								for _, p := range patterns {
									// does value start with p
									if strings.HasPrefix(node.Data.Map[collection][i][k.(string)].(string), p) {
										if skip != 0 {
											skip = skip - 1
											goto sk
										}
										conditionsMetDocument += 1

									sk:
										continue
									}
								}
							} else {
								r := regexp.MustCompile(`\%(.*)`)
								patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

								for j := range patterns {
									patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
								}

								for _, p := range patterns {
									// does value end with p
									if strings.HasSuffix(node.Data.Map[collection][i][k.(string)].(string), p) {
										if skip != 0 {
											skip = skip - 1
											goto sk2
										}
										conditionsMetDocument += 1

									sk2:
										continue
									}
								}
							}
						} else {

							r := regexp.MustCompile(`%(.*?)%`)
							patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

							for j := range patterns {
								patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
							}

							for _, p := range patterns {
								// does value contain p
								if strings.Count(node.Data.Map[collection][i][k.(string)].(string), p) > 0 {
									if skip != 0 {
										skip = skip - 1
										goto sk3
									}
									conditionsMetDocument += 1

								sk3:
									continue
								}
							}
						}
					} else if oprs.([]interface{})[m] == "!like" {
						vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
						vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
						vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
						vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
						vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
						vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

						// select * from users where firstName not like 'alex%'
						if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
							// Get index of % and check if on left or right of string
							percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
							sMiddle := len(vs.([]interface{})[m].(string)) / 2
							right := sMiddle <= percIndex

							if right {
								r := regexp.MustCompile(`^(.*?)%`)
								patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

								for j := range patterns {
									patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
								}

								for _, p := range patterns {
									// does value start with p

									if !strings.HasPrefix(node.Data.Map[collection][i][k.(string)].(string), p) {
										if skip != 0 {
											skip = skip - 1
											goto sk4
										}
										conditionsMetDocument += 1

									sk4:
										continue
									}
								}
							} else {
								r := regexp.MustCompile(`\%(.*)`)
								patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

								for j := range patterns {
									patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
								}

								for _, p := range patterns {
									// does value end with p
									if !strings.HasSuffix(node.Data.Map[collection][i][k.(string)].(string), p) {
										if skip != 0 {
											skip = skip - 1
											goto sk5
										}
										conditionsMetDocument += 1

									sk5:
										continue
									}
								}
							}
						} else {

							r := regexp.MustCompile(`%(.*?)%`)
							patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

							for j := range patterns {
								patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
							}

							for _, p := range patterns {
								// does value contain p
								if strings.Count(node.Data.Map[collection][i][k.(string)].(string), p) == 0 {
									if skip != 0 {
										skip = skip - 1
										goto sk6
									}
									conditionsMetDocument += 1

								sk6:
									continue
								}
							}
						}
					} else if oprs.([]interface{})[m] == "==" {
						if reflect.DeepEqual(node.Data.Map[collection][i][k.(string)], vs.([]interface{})[m]) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}
					} else if oprs.([]interface{})[m] == "!=" {
						if !reflect.DeepEqual(node.Data.Map[collection][i][k.(string)], vs.([]interface{})[m]) {

							(func() {

								if skip != 0 {
									skip = skip - 1
									goto exists
								}
								conditionsMetDocument += 1
							exists:
							})()

						}
					}

				}
			}
		}

		if slices.Contains(conditions, "&&") {
			if conditionsMetDocument >= len(conditions) {
				if update {
					node.Data.Map[collection][i]["$indx"] = i
				}

				mu.Lock()
				if len(*objs) == vol {
					mu.Unlock()
					return
				}

				*objs = append(*objs, node.Data.Map[collection][i])
				if del {
					*tbd = append(*tbd, i)
				}
				mu.Unlock()

			} else if slices.Contains(conditions, "||") && conditionsMetDocument > 0 {
				if update {
					node.Data.Map[collection][i]["$indx"] = i
				}

				mu.Lock()
				if len(*objs) == vol {
					mu.Unlock()
					return
				}

				*objs = append(*objs, node.Data.Map[collection][i])
				if del {
					*tbd = append(*tbd, i)
				}
				mu.Unlock()
			}
		} else if slices.Contains(conditions, "||") && conditionsMetDocument > 0 {
			if update {
				node.Data.Map[collection][i]["$indx"] = i
			}

			mu.Lock()
			if len(*objs) == vol {
				mu.Unlock()
				return
			}

			*objs = append(*objs, node.Data.Map[collection][i])
			if del {
				*tbd = append(*tbd, i)
			}
			mu.Unlock()
		} else if conditionsMetDocument > 0 && len(conditions) == 1 {
			if update {
				node.Data.Map[collection][i]["$indx"] = i
			}

			mu.Lock()
			if len(*objs) == vol {
				mu.Unlock()
				return
			}

			*objs = append(*objs, node.Data.Map[collection][i])
			if del {
				*tbd = append(*tbd, i)
			}
			mu.Unlock()
		}

	}

}

// Select is the node data select method
func (node *Node) Select(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}, del bool, sortPos string, sortKey string, count bool, update bool) []interface{} {
	// sortPos = desc OR asc
	// sortKey = createdAt for example a unix timestamp of 1703234712 or firstName with a value of Alex sorting will sort alphabetically

	// If a lock was sent from cluster lock the collection on this read
	if lock {
		l, ok := node.Data.Writers[collection]
		if ok {
			l.Lock()
		}

	}

	// Unlock when completed, by defering
	defer func() {
		if lock {
			l, ok := node.Data.Writers[collection]
			if ok {
				l.Unlock()
			}
		}
	}()

	// Results
	var objects []interface{}

	// To be deleted
	var tbd []int

	//The && operator displays a document if all the conditions are TRUE.
	//The || operator displays a record if any of the conditions are TRUE.

	searchWg := &sync.WaitGroup{}
	searchResMu := &sync.RWMutex{}

	if len(node.Data.Map[collection]) >= 60 && skip == 0 { // If collection has more than 60 records and there is no skip split search

		// Split collection and conquer from top to bottom in parallel
		middle := len(node.Data.Map[collection]) / 2

		// top to middle search
		searchWg.Add(1)
		go func(wg *sync.WaitGroup, mid int, objs *[]interface{}, mu *sync.RWMutex, v int) {
			defer wg.Done()
			for i := 0; i <= mid; i++ {
				if node.Context.Err() != nil {
					return
				}

				node.Search(searchResMu, i, &tbd, collection, ks, vs, v, skip, oprs, conditions, del, update, objs)
			}
		}(searchWg, middle, &objects, searchResMu, vol)

		// bottom to middle search
		searchWg.Add(1)
		go func(wg *sync.WaitGroup, mid int, objs *[]interface{}, mu *sync.RWMutex, v int) {
			defer wg.Done()
			for i := len(node.Data.Map[collection]) - 1; i > mid; i-- {
				if node.Context.Err() != nil {
					return
				}

				node.Search(searchResMu, i, &tbd, collection, ks, vs, v, skip, oprs, conditions, del, update, objs)
			}
		}(searchWg, middle, &objects, searchResMu, vol)

		searchWg.Wait()
	} else {
		for i := range node.Data.Map[collection] {
			if skip != 0 {
				skip = skip - 1
				continue
			}
			node.Search(searchResMu, i, &tbd, collection, ks, vs, vol, skip, oprs, conditions, del, update, &objects)
		}
	}

	// Should only sort integers, floats and strings
	if sortKey != "" && sortPos != "" {

		for _, d := range objects {

			doc, ok := d.(map[string]interface{})[sortKey]
			if ok {
				if reflect.TypeOf(doc).Kind().String() == "string" {
					// alphabetical sorting based on string[0] value A,B,C asc C,B,A desc
					sort.Slice(objects[:], func(z, x int) bool {
						if sortPos == "asc" {
							return objects[z].(map[string]interface{})[sortKey].(string) < objects[x].(map[string]interface{})[sortKey].(string)
						} else {
							return objects[z].(map[string]interface{})[sortKey].(string) > objects[x].(map[string]interface{})[sortKey].(string)
						}
					})
				} else if reflect.TypeOf(d.(map[string]interface{})[sortKey]).Kind().String() == "float64" {
					// numerical sorting based on float64[0] value 1.1,1.0,0.9 desc 0.9,1.0,1.1 asc
					sort.Slice(objects[:], func(z, x int) bool {
						if sortPos == "asc" {
							return objects[z].(map[string]interface{})[sortKey].(float64) < objects[x].(map[string]interface{})[sortKey].(float64)
						} else {
							return objects[z].(map[string]interface{})[sortKey].(float64) > objects[x].(map[string]interface{})[sortKey].(float64)
						}
					})
				} else if reflect.TypeOf(d.(map[string]interface{})[sortKey]).Kind().String() == "int" {
					// numerical sorting based on int[0] value 22,12,3 desc 3,12,22 asc
					sort.Slice(objects[:], func(z, x int) bool {
						if sortPos == "asc" {
							return objects[z].(map[string]interface{})[sortKey].(int) < objects[x].(map[string]interface{})[sortKey].(int)
						} else {
							return objects[z].(map[string]interface{})[sortKey].(int) > objects[x].(map[string]interface{})[sortKey].(int)
						}
					})
				}

			}

		}
	}

	if count {
		var countResponse []interface{}
		countObject := make(map[string]interface{})

		countObject["count"] = len(objects)
		countResponse = append(countResponse, countObject)
		return countResponse
	}

	if len(tbd) > 0 {
		sort.Ints(tbd) // sort in order
		node.Data.Writers[collection].Lock()
		for j := len(tbd) - 1; j >= 0; j-- {
			copy(node.Data.Map[collection][tbd[j]:], node.Data.Map[collection][tbd[j]+1:])
			node.Data.Map[collection][len(node.Data.Map[collection])-1] = nil
			node.Data.Map[collection] = node.Data.Map[collection][:len(node.Data.Map[collection])-1]

			// if no entries in collection, remove it.
			if len(node.Data.Map[collection]) == 0 {
				delete(node.Data.Map, collection)
			}
		}
		node.Data.Writers[collection].Unlock()
	}

	return objects
}

// Delete is the node data delete method
func (node *Node) Delete(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}, sortPos string, sortKey string) []interface{} {
	var deleted []interface{}
	for _, doc := range node.Select(collection, ks, vs, vol, skip, oprs, lock, conditions, true, sortPos, sortKey, false, false) {
		deleted = append(deleted, doc)
	}

	return deleted
}

// Update is the node data update method
func (node *Node) Update(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}, uks []interface{}, nvs []interface{}, sortPos string, sortKey string) []interface{} {
	var updated []interface{}
	for _, doc := range node.Select(collection, ks, vs, vol, skip, oprs, lock, conditions, false, sortPos, sortKey, false, true) {
		node.Data.Writers[collection].Lock()
		ne := make(map[string]interface{})

		indx := 0

		for kk, vv := range doc.(map[string]interface{}) {
			if kk == "$indx" {
				indx = vv.(int)
			} else {
				ne[kk] = vv
			}
		}

		for m := range uks {

			ne[uks[m].(string)] = nvs[m]

		}

		node.Data.Map[collection][indx] = ne
		updated = append(updated, node.Data.Map[collection][indx])
		node.Data.Writers[collection].Unlock()
	}

	return updated
}
