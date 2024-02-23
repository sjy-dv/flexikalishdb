package architecture

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/sjy-dv/flexikalishdb/compression"
)

func (node *Node) GetLogs(r io.Reader) int {
	buf := make([]byte, 32*1024)
	cnt := 0
	line := []byte{'\n'}
	for {
		c, err := r.Read(buf)
		cnt += bytes.Count(buf[:c], line)

		switch {
		case err == io.EOF:
			return cnt

		case err != nil:
			node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
			return 99999999
		}
	}
}

func (node *Node) PLof(data, level string) {
	if node.Config.Logging {
		if node.GetLogs(node.LogFile)+1 >= node.Config.LogMaxLines {
			node.LogMu.Lock()
			defer node.LogMu.Unlock()
			node.LogFile.Close()
			err := os.Truncate(node.LogFile.Name(), 0)
			if err != nil {
				node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] PLof(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
				return
			}

			tz, err := time.LoadLocation(node.Config.Timezone)
			if err != nil {
				node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] PLof(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not use configured timezone", err.Error())))
				return
			}

			node.LogFile, err = os.OpenFile("node.log", os.O_CREATE|os.O_RDWR, 0777)
			if err != nil {
				return
			}
			node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), fmt.Sprintf("Log truncated at %d", node.Config.LogMaxLines))))
			node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), data)))
		} else {
			tz, err := time.LoadLocation(node.Config.Timezone)
			if err != nil {
				node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] PLof(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not use configured timezone", err.Error())))
				return
			}

			node.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), data)))
		}

	} else {
		log.Println(fmt.Sprintf("[%s] %s", level, data))
	}
}

func (node *Node) WriteToFile(backup bool) {
	var t time.Time // Time for if backup is enabled

	if backup {
		tz, err := time.LoadLocation(node.Config.Timezone)
		if err != nil {
			t = time.Now()
		} else {
			t = time.Now().In(tz)
		}

	}

	if !backup {
		node.PLof(fmt.Sprintf("WriteToFile(): %d Starting to write node data to file.", 220), "INFO")
	} else {
		node.PLof(fmt.Sprintf("WriteToFile(): %d Starting to write node data to backup file.", 221), "INFO")
	}

	if !backup {
		var out io.Writer

		f, err := os.OpenFile(".cdat", os.O_TRUNC|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
		if err != nil {
			node.PLof(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
			node.SignalChannel <- os.Interrupt
			return
		}

		decodedKey, err := base64.StdEncoding.DecodeString(node.Config.Key)
		if err != nil {
			node.PLof(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
			node.SignalChannel <- os.Interrupt
			return
		}
		out, _ = compression.NewWriter(f, compression.StrongCompression, decodedKey)

		enc := gob.NewEncoder(out)

		enc.Encode(node.Data.Map)

		out.(io.Closer).Close()
	} else {
		var out io.Writer

		var f *os.File
		var err error

		if runtime.GOOS == "windows" {
			f, err = os.OpenFile(fmt.Sprintf("%s\\.cdat.%d", node.Config.BackupsDirectory, t.Unix()), os.O_TRUNC|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
			if err != nil {
				node.PLof(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
				node.SignalChannel <- os.Interrupt
				return
			}
		} else {
			f, err = os.OpenFile(fmt.Sprintf("%s/.cdat.%d", node.Config.BackupsDirectory, t.Unix()), os.O_TRUNC|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
			if err != nil {
				node.PLof(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
				node.SignalChannel <- os.Interrupt
				return
			}
		}

		decodedKey, err := base64.StdEncoding.DecodeString(node.Config.Key)
		if err != nil {
			node.PLof(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
			node.SignalChannel <- os.Interrupt
			return
		}
		out, _ = compression.NewWriter(f, compression.StrongCompression, decodedKey)

		enc := gob.NewEncoder(out)

		enc.Encode(node.Data.Map)

		out.(io.Closer).Close()

	}

	if !backup {
		node.PLof(fmt.Sprintf("WriteToFile(): %d Node data written to file successfully.", 222), "INFO")
	} else {
		node.PLof(fmt.Sprintf("WriteToFile(): %d Node data written to backup file successfully.", 223), "INFO")
	}

}
