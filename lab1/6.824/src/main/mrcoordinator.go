package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		for file, _ := range m.Files {
			if m.Files[file] == mr.TASKPROCESSING {
				m.Files[file] = mr.TASKREADDY
			}
		}
		time.Sleep(10 * time.Second)
	}

	for m.ReduceDone() == false {
		for file, _ := range m.Files {
			if m.Files[file] == mr.TASKPROCESSING {
				m.Files[file] = mr.TASKREADDY
			}
		}
		time.Sleep(20 * time.Second)
	}

	return
}
