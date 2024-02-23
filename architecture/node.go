package architecture

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/sjy-dv/flexikalishdb/compression"
	"golang.org/x/crypto/ssh/terminal"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

func (node *Node) CurrentMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.Alloc / 1024 / 1024
}

func (node *Node) RenewNodeConfig() error {
	nodeCfg, err := os.ReadFile("./.nodeconfig")
	if err != nil {
		createError := fmt.Sprintf("main(): %d Could not open/create configuration file ", 118) + err.Error()
		node.PLof(createError, "FATAL")
		return errors.New(createError)
	}
	err = yaml.Unmarshal(nodeCfg, &node.Config)
	if err != nil {
		createError := fmt.Sprintf("main(): %d Could not unmarshal system yaml configuration ", 113) + err.Error()
		node.PLof(createError, "FATAL")
		return errors.New(createError)
	}
	if node.Config.Logging {
		node.LogMu = &sync.Mutex{} // Cluster node log mutex
		node.LogFile, err = os.OpenFile("node.log", os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			createError := fmt.Sprintf("main(): %d Could not open log file ", 110) + err.Error()
			node.PLof(createError, "FATAL")
			return errors.New(createError)
		}
	}

	return nil

}

// SetupNodeConfig sets up default cluster node config i.e .nodeconfig
func (node *Node) SetupNodeConfig() error {
	// Create .nodeconfig
	nodeConfigFile, err := os.OpenFile("./.nodeconfig", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		errMsg := fmt.Sprintf("SetupNodeConfig(): %d Could not open/create configuration file ", 118) + err.Error()
		node.PLof(errMsg, "FATAL")
		return errors.New(errMsg)
	}

	// Defer close node config
	defer nodeConfigFile.Close()

	// SETTING DEFAULTS
	///////////////////////////////////
	node.Config.Port = 5790                      // Set default CursusDB node port
	node.Config.MaxMemory = 10240                // Max memory 10GB default
	node.Config.Host = "0.0.0.0"                 // Set default host of 0.0.0.0
	node.Config.LogMaxLines = 1000               // truncate at 1000 lines as default
	node.Config.Timezone = "Local"               // Local is systems local time
	node.Config.ReplicationSyncTime = 10         // default of every 10 minutes
	node.Config.ReplicationSyncTimeout = 10      // If sync doesn't complete in 10 minutes by default timeout(could lead to corrupt data so increase accordingly)
	node.Config.AutomaticBackupCleanupHours = 12 // Set default of 12 hours in which to delete old backed up .cdat files
	node.Config.AutomaticBackupTime = 60         // Automatically backup node data to backups folder every 1 hour by default if AutomaticBackups is enabled

	if runtime.GOOS == "windows" {
		node.Config.BackupsDirectory = ".\\backups" // Backups by default is in the execution directory (windows)
	} else {
		node.Config.BackupsDirectory = "./backups" // Backups by default is in the execution directory
	}

	fmt.Println("Shared cluster and node key is required.  A shared cluster and node key will encrypt all your data at rest and only allow connections that contain a correct Key: header value matching the hashed key you provide.")
	fmt.Print("key> ")
	var key []byte

	if terminal.IsTerminal(int(syscall.Stdin)) {
		key, err = term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			errMsg := fmt.Sprintf("SetupNodeConfig(): %s", err.Error())
			node.PLof(errMsg, "FATAL") // No need to report status code this should be pretty apparent to troubleshoot for a user and a developer
			return errors.New(errMsg)
		}
	} else {
		reader := bufio.NewReader(os.Stdin)
		key, _, _ = reader.ReadLine()
	}

	// Repeat key with * so Alex would be ****
	fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(key))))
	fmt.Println("")

	// Hash and encode key
	hashedKey := sha256.Sum256(key)
	node.Config.Key = base64.StdEncoding.EncodeToString(append([]byte{}, hashedKey[:]...))

	// Marshal node config into yaml
	yamlData, err := yaml.Marshal(&node.Config)
	if err != nil {
		errMsg := fmt.Sprintf("SetupNodeConfig(): %d Could not marshal system yaml configuration ", 114) + err.Error()
		node.PLof(errMsg, "FATAL")
		return errors.New(errMsg)
	}

	// Write to node config
	nodeConfigFile.Write(yamlData)

	return nil
}

// SetupInitializeCDat reads .cdat file and will reinstate data back into memory.  On failure and backups are setup this method will also automatically recover if corrupt or try to.
func (node *Node) SetupInitializeCDat() error {
	if _, err := os.Stat(fmt.Sprintf("%s", ".cdat")); errors.Is(err, os.ErrNotExist) { // Not exists we create it
		node.PLof(fmt.Sprintf("SetupInitializeCDat(): %d No previous data to read.  Creating new .cdat file.", 109), "INFO")
	} else {

		datafile := "./.cdat"        // If .cdat is corrupted we will try again with a backup
		var latestBackup fs.FileInfo // When we search BackupsDirectory we look for latest backup also we use this variable to check if we already attempted to use this backup
		backupCount := 0             // Will populate then decrement when we read backups directory if the directory exists
		backedUp := false            // If a backup occurred

		goto readData

	readData:

		cdat, err := os.OpenFile(fmt.Sprintf(datafile), os.O_RDONLY, 0777)
		if err != nil {
			errMsg := "SetupInitializeCDat(): " + fmt.Sprintf("%d Could not open/create data file ", 119) + err.Error()
			node.PLof(errMsg, "FATAL")
			return errors.New(errMsg)
		}

		var in io.Reader

		decodedKey, err := base64.StdEncoding.DecodeString(node.Config.Key)
		if err != nil {
			errMsg := "SetupInitializeCDat(): " + fmt.Sprintf("%d Could not decode configured shared key. ", 115) + err.Error()
			node.PLof(errMsg, "FATAL")
			return errors.New(errMsg)
		}

		in = compression.NewReader(cdat, decodedKey)

		dec := gob.NewDecoder(in)

		err = dec.Decode(&node.Data.Map)
		if err != nil {
			goto corrupt // Data is no good.  Try to recover on backup
		}

		in.(io.Closer).Close()

		goto ok

	corrupt:
		node.PLof(fmt.Sprintf("SetupInitializeCDat(): %d Data file corrupt! %s", 111, err.Error()), "WARNING")
		os.Remove(fmt.Sprintf("%s.tmp", datafile))
		// Data file is corrupt.. If node has backups configured grab last working state.

		if node.Config.AutomaticBackups {
			node.PLof(fmt.Sprintf("SetupInitializeCDat(): %d Attempting automatic recovery with latest backup.", 215), "INFO")

			// Read backups and remove any backups older than AutomaticBackupCleanupTime days old
			rbackups, err := os.ReadDir(fmt.Sprintf("%s", node.Config.BackupsDirectory))
			if err != nil {
				errMsg := fmt.Sprintf("SetupInitializeCDat(): %d Could not read node backups directory %s", 208, err.Error())
				node.PLof(errMsg, "FATAL")
				return errors.New(errMsg)
			}

			backups, err := node.CvFsInfo(rbackups)
			if err != nil {
				return err
			}
			if backupCount == 0 {
				backupCount = len(backups)
			}

			for _, backup := range backups {
				backedUp = true
				if latestBackup == nil {
					latestBackup = backup
				} else {
					if backup.ModTime().Before(latestBackup.ModTime()) && latestBackup.Name() != backup.Name() {
						latestBackup = backup
					}
				}
			}

			if backupCount != 0 {
				backupCount -= 1
				if runtime.GOOS == "windows" {
					datafile = fmt.Sprintf("%s\\%s", node.Config.BackupsDirectory, latestBackup.Name())
				} else {
					datafile = fmt.Sprintf("%s/%s", node.Config.BackupsDirectory, latestBackup.Name())
				}
				goto readData
			} else {
				errMsg := fmt.Sprintf("SetupInitializeCDat(): %d Node was unrecoverable after all attempts.", 214)
				node.PLof(errMsg, "FATAL")
				return errors.New(errMsg)
			}

		} else {
			errMsg := fmt.Sprintf("SetupInitializeCDat(): %d Node was unrecoverable after all attempts.", 214)
			node.PLof(errMsg, "FATAL")
			return errors.New(errMsg)
		}

	ok:

		if backedUp { // RECOVERED
			node.PLof(fmt.Sprintf("SetupInitializeCDat(): %d Node recovery from backup was successful.", 211), "INFO")
		}

		cdat.Close()

		// Setup collection mutexes
		for c, _ := range node.Data.Map {
			node.Data.Writers[c] = &sync.RWMutex{}
		}

		node.PLof(fmt.Sprintf("SetupInitializeCDat(): %d Collection mutexes created.", 112), "INFO")
	}

	return nil
}

// SignalListener listens for system signals
func (node *Node) SignalListener() {
	defer node.Wg.Done()
	for {
		select {
		case sig := <-node.SignalChannel:
			node.PLof(fmt.Sprintf("SignalListener(): %d Received signal %s starting database shutdown.", -1, sig), "INFO")

			// Close observer connections if any
			for _, oc := range node.ObserverConnections {
				oc.Text.Close()
				oc.Conn.Close()
			}

			node.TCPListener.Close() // Close up TCP/TLS listener
			node.ContextCancel()     // Cancel context, used for loops and so forth
			return
		default:
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}

// SyncOut syncs current data to replicas at configured interval
func (node *Node) SyncOut() {
	defer node.Wg.Done()

	stateCh := make(chan int)
	// 0 - continue
	// 1 - sleep
	// 2 - cancel

	go func(c *Node, sc chan int) {
		f := time.Now().Add(time.Minute * time.Duration(node.Config.ReplicationSyncTime))
		for {
			if c.Context.Err() != nil {
				sc <- 2
				return
			}

			if time.Now().After(f) {
				f = time.Now().Add(time.Minute * time.Duration(node.Config.ReplicationSyncTime))
				sc <- 0
				time.Sleep(time.Nanosecond * 1000000)
			} else {
				sc <- 1
				time.Sleep(time.Nanosecond * 1000000)
			}
		}
	}(node, stateCh)

	for {
		select {
		case sc := <-stateCh:

			if sc == 0 {
				for _, r := range node.Config.Replicas {

					if node.Config.TLSReplication {
						config := tls.Config{ServerName: r.Host} // i.e node-replica2.example.io

						conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", r.Host, r.Port), &config)
						if err != nil {
							node.PLof("SyncOut():"+err.Error(), "ERROR")
							continue
						}

						// Authenticate with node passing shared key wrapped in base64
						conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", node.Config.Key)))

						// Authentication response buffer
						authBuf := make([]byte, 1024)

						// Read response back from node
						re, _ := conn.Read(authBuf[:])

						// Did response start with a 0?  This indicates successful authentication
						if strings.HasPrefix(string(authBuf[:re]), "0") {
							conn.Write([]byte(fmt.Sprintf("SYNC DATA\r\n")))
							syncDataResponseBuf := make([]byte, 1024)

							// Read response back from node
							syncDataResponse, _ := conn.Read(syncDataResponseBuf[:])
							if strings.HasPrefix(string(syncDataResponseBuf[:syncDataResponse]), "106") {

								e := gob.NewEncoder(conn)

								err = e.Encode(&node.Data.Map)
								if err != nil {
									conn.Close()
									node.PLof(fmt.Sprintf("SyncOut(): %d Could not encode data for sync. %s", 219, err.Error()), "ERROR")
									break
								}

								syncFinishResponseBuf := make([]byte, 1024)

								// Read response back from node
								syncFinishResponse, _ := conn.Read(syncFinishResponseBuf[:])

								node.PLof("SyncOut(): "+string(syncFinishResponseBuf[:syncFinishResponse]), "INFO")

								conn.Close()
							}
						} else {
							node.PLof("SyncOut():"+fmt.Sprintf("%d Failed node sync auth %s", 5, string(authBuf[:re])), "ERROR")
						}
					} else {
						// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
						tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", r.Host, r.Port))
						if err != nil {
							node.PLof(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
							continue
						}

						// Dial tcp address up
						conn, err := net.DialTCP("tcp", nil, tcpAddr)
						if err != nil {
							node.PLof(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
							continue
						}

						// Authenticate with node passing shared key wrapped in base64
						conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", node.Config.Key)))

						// Authentication response buffer
						authBuf := make([]byte, 1024)

						// Read response back from node
						re, _ := conn.Read(authBuf[:])

						// Did response start with a 0?  This indicates successful authentication
						if strings.HasPrefix(string(authBuf[:re]), "0") {
							conn.Write([]byte(fmt.Sprintf("SYNC DATA\r\n")))
							syncDataResponseBuf := make([]byte, 1024)

							// Read response back from node
							syncDataResponse, _ := conn.Read(syncDataResponseBuf[:])
							if strings.HasPrefix(string(syncDataResponseBuf[:syncDataResponse]), "106") {

								e := gob.NewEncoder(conn)

								err = e.Encode(&node.Data.Map)
								if err != nil {
									conn.Close()
									node.PLof(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
									break
								}

								syncFinishResponseBuf := make([]byte, 1024)

								// Read response back from node
								syncFinishResponse, _ := conn.Read(syncFinishResponseBuf[:])

								node.PLof("SyncOut(): "+string(syncFinishResponseBuf[:syncFinishResponse]), "INFO")

								conn.Close()
							}
						} else {
							node.PLof("SyncOut():"+fmt.Sprintf("%d Failed node sync auth %s", 5, string(authBuf[:re])), "ERROR")
						}
					}

				}
			} else if sc == 2 {
				return
			} else if sc == 1 {
				time.Sleep(time.Nanosecond * 1000000)
				continue
			}
		default:
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}
