package cluster

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// CountLog counts amount of lines within log file
func (cluster *ClusterNode) CountLog(r io.Reader) int {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count

		case err != nil:
			cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
			return 99999999
		}
	}
}

// Printl prints a line to the cluster.log file also will clear at LogMaxLines.
// Appropriate levels: ERROR, INFO, FATAL, WARN
func (cluster *ClusterNode) Printl(data string, level string) {
	if cluster.Config.Logging {
		if cluster.CountLog(cluster.LogFile)+1 >= cluster.Config.LogMaxLines {
			cluster.LogMu.Lock()
			defer cluster.LogMu.Unlock()
			cluster.LogFile.Close()
			err := os.Truncate(cluster.LogFile.Name(), 0)
			if err != nil {
				cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] Printl(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
				return
			}

			tz, err := time.LoadLocation(cluster.Config.Timezone)
			if err != nil {
				cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not use configured timezone", err.Error())))
				return
			}

			cluster.LogFile, err = os.OpenFile("cluster.log", os.O_CREATE|os.O_RDWR, 0777)
			if err != nil {
				return
			}
			cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), fmt.Sprintf("Log truncated at %d", cluster.Config.LogMaxLines))))
			cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), data)))
		} else {
			tz, err := time.LoadLocation(cluster.Config.Timezone)
			if err != nil {
				cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] Printl(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not use configured timezone", err.Error())))
				return
			}

			cluster.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), data)))
		}
	} else {
		log.Println(fmt.Sprintf("[%s] %s", level, data))
	}

}
