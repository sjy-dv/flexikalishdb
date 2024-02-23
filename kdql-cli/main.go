package main

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/peterh/liner"
	"golang.org/x/term"
)

type KdqlCli struct {
	TLS         bool
	ClusterHost string
	ClusterPort int
}

var (
	history_fn = filepath.Join(os.TempDir(), ".query_history")
)

func main() {
	var kdql KdqlCli

	err := kdql.RunShell()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}

func (kdql *KdqlCli) RunShell() error {
	flag.BoolVar(&kdql.TLS, "tls", false, "Use secure connection.")
	flag.StringVar(&kdql.ClusterHost, "host", "", "Cluster host.")
	flag.IntVar(&kdql.ClusterPort, "port", 4701, "Cluster host port.")
	flag.Parse()

	if kdql.ClusterHost == "" {
		errMsg := "FlexiKalish cluster host required."
		return errors.New(errMsg)
	}

	if kdql.ClusterPort == 0 {
		errMsg := "FlexiKalish cluster host port required."
		return errors.New(errMsg)
	}

	if !kdql.TLS {

		fmt.Print("Username>")
		username, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(username))))

		fmt.Println("")
		fmt.Print("Password>")
		password, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(password))))

		tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", kdql.ClusterHost, kdql.ClusterPort))
		if err != nil {
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		defer conn.Close()

		text := textproto.NewConn(conn)
		// Authenticate
		err = text.PrintfLine(fmt.Sprintf("Authentication: %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s\\0%s", username, password)))))
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		read, err := text.ReadLine()
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		if strings.HasPrefix(read, fmt.Sprintf("%d ", 0)) {

			query := ""

			line := liner.NewLiner()
			defer line.Close()

			line.SetCtrlCAborts(true)

			if f, err := os.Open(history_fn); err == nil {
				line.ReadHistory(f)
				f.Close()
			}
			fmt.Println("")
			for {
				if in, err := line.Prompt("kdql>"); err == nil {
					query += in

					query = strings.Join(strings.Split(query, " "), " ")

					if strings.HasSuffix(query, ";") {
						line.AppendHistory(query)
						_, err = conn.Write([]byte(strings.TrimSpace(query) + "\r\n"))
						if err != nil {
							fmt.Println("")
							errMsg := err.Error()
							return errors.New(errMsg)
						}

						read, err := text.ReadLine()
						if err != nil {
							fmt.Println("")
							errMsg := err.Error()
							return errors.New(errMsg)
						}
						fmt.Println(read)
						query = ""
					}

				} else if err == liner.ErrPromptAborted {
					fmt.Println("")
					fmt.Println("Aborted")
					break
				} else {
					fmt.Println("")
					fmt.Println("Error reading line: ", err)
					break
				}
			}
		} else {
			fmt.Println("")
			fmt.Println("Invalid credentials.")
		}

	} else {

		fmt.Print("Username>")
		username, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(username))))
		fmt.Println("")
		fmt.Print("Password>")
		password, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(password))))

		config := tls.Config{ServerName: kdql.ClusterHost}

		conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", kdql.ClusterHost, kdql.ClusterPort), &config)
		if err != nil {
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		defer conn.Close()

		text := textproto.NewConn(conn)
		// Authenticate
		err = text.PrintfLine(fmt.Sprintf("Authentication: %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s\\0%s", username, password)))))
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		read, err := text.ReadLine()
		if err != nil {
			fmt.Println("")
			errMsg := err.Error()
			return errors.New(errMsg)
		}

		if strings.HasPrefix(read, fmt.Sprintf("%d ", 0)) {

			query := ""

			line := liner.NewLiner()
			defer line.Close()

			line.SetCtrlCAborts(true)

			if f, err := os.Open(history_fn); err == nil {
				line.ReadHistory(f)
				f.Close()
			}
			fmt.Println("")
			for {
				if in, err := line.Prompt("kdql>"); err == nil {
					query += in

					query = strings.Join(strings.Split(query, " "), " ")

					if strings.HasSuffix(query, ";") {
						line.AppendHistory(query)
						_, err = conn.Write([]byte(strings.TrimSpace(query) + "\r\n"))
						//err = text.PrintfLine(query) // Because of % we should not use printf
						if err != nil {
							fmt.Println("")
							errMsg := err.Error()
							return errors.New(errMsg)
						}

						read, err := text.ReadLine()
						if err != nil {
							fmt.Println("")
							errMsg := err.Error()
							return errors.New(errMsg)
						}
						fmt.Println(read)
						query = ""
					}

				} else if err == liner.ErrPromptAborted {
					fmt.Println("")
					fmt.Println("Aborted")
					break
				} else {
					fmt.Println("")
					fmt.Println("Error reading line: ", err)
					break
				}
			}
		} else {
			fmt.Println("")
			fmt.Println("Invalid credentials.")
		}

	}

	return nil
}
