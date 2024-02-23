package architecture

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// AutomaticBackup automatically backs up node data every set AutomaticBackupTime hours to node backups directory under .cdat.unixtime
// Also will remove any old backups based on the days provided on AutomaticBackupCleanupTime
func (node *Node) AutomaticBackup() {
	defer node.Wg.Done()

	// Check if BackupsDirectory exists or not and create it
	if _, err := os.Stat(fmt.Sprintf("%s", node.Config.BackupsDirectory)); errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(fmt.Sprintf("%s", node.Config.BackupsDirectory), os.ModePerm); err != nil {
			node.PLof(fmt.Sprintf("AutomaticBackup(): %d Could not create automatic backups directory %s", 207, err.Error()), "ERROR")
			return
		}
	}

	stateCh := make(chan int)
	// 0 - continue
	// 1 - sleep
	// 2 - cancel

	go func(c *Node, sc chan int) {
		f := time.Now().Add(time.Minute * time.Duration(node.Config.AutomaticBackupTime))
		for {
			if c.Context.Err() != nil {
				sc <- 2
				return
			}

			if time.Now().After(f) { // time to backup!
				f = time.Now().Add(time.Minute * time.Duration(node.Config.AutomaticBackupTime))
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
		case sc := <-stateCh: // State channel, either continue(run), sleep(wait), or cancel(drop)

			if sc == 0 { // run

				// Backup data to BackupsDirectory.cdat.unixtime
				node.WriteToFile(true)
				// entries, err := os.ReadDir(dirname)
				// if err != nil { ... }
				// infos := make([]fs.FileInfo, 0, len(entries))
				// for _, entry := range entries {
				// 	info, err := entry.Info()
				// 	if err != nil { ... }
				// 	infos = append(infos, info)
				// }
				// Read backups and remove any backups older than AutomaticBackupCleanupTime days old
				rbackups, err := os.ReadDir(fmt.Sprintf("%s", node.Config.BackupsDirectory)) // read backups directory
				if err != nil {
					node.PLof(fmt.Sprintf("AutomaticBackup(): %d Could not read node backups directory %s", 208, err.Error()), "ERROR")
					time.Sleep(time.Second)
					continue
				}
				// backups := make([]fs.FileInfo, 0, len(rbackups))
				// for _, rb := range rbackups {
				// 	info, err := rb.Info()
				// 	if err != nil {
				// 		node.PLof(fmt.Sprintf("AutomaticBackup(): %d Could not read node backups directory Info %s", 208, err.Error()), "ERROR")
				// 		time.Sleep(time.Second)
				// 		return
				// 	}
				// 	backups = append(backups, info)
				// }
				backups, err := node.CvFsInfo(rbackups)
				if err != nil {
					continue
				}
				for _, backup := range backups {
					if backup.ModTime().After(time.Now().Add(time.Hour * time.Duration(node.Config.AutomaticBackupCleanupHours))) {
						e := os.Remove(fmt.Sprintf("%s%s", node.Config.BackupsDirectory, backup.Name()))
						if e != nil {
							node.PLof(fmt.Sprintf("AutomaticBackup(): %d Could not remove .cdat backup %s %s", 209, backup.Name(), err.Error()), "ERROR")
							continue
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

func (node *Node) CvFsInfo(entry []fs.DirEntry) ([]fs.FileInfo, error) {

	infos := make([]fs.FileInfo, 0, len(entry))
	for _, entry := range entry {
		info, err := entry.Info()
		if err != nil {
			node.PLof(fmt.Sprintf("AutomaticBackup(): %d Could not read node backups directory %s", 208, err.Error()), "ERROR")
			time.Sleep(time.Second)
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}
