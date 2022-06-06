package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

var (
	TASKREADDY     = 0
	TASKPROCESSING = 1
	TASKDONE       = 2
)

type Coordinator struct {
	Files            map[string]int //record time of task sent out
	NReduce          int
	ReduceTaskStasus map[string]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// //work call this func to tell that a sub task is done

func (c *Coordinator) SubTaskDone(args *SubTaskDoneArgs, reply *SubTaskDoneReply) error {
	//set task status as done
	if _, ok := c.Files[args.FileName]; ok {
		c.Files[args.FileName] = TASKDONE
	}
	return nil
}

func (c *Coordinator) DispatchTask(args *DispatchArgs, reply *DispatchReply) error {
	//set task status as TASKPROCESSING
	for file, _ := range c.Files {
		if c.Files[file] == TASKREADDY {
			c.Files[file] = TASKPROCESSING
			reply.FileName = file
			reply.NReduce = c.NReduce
			return nil
		}
	}
	return nil
}

func (c *Coordinator) DispatchReduceTask(args *DispatchReduceTaskArgs, reply *DispatchReduceTaskReply) error {
	for file, _ := range c.ReduceTaskStasus {
		if c.ReduceTaskStasus[file] == TASKREADDY {
			c.ReduceTaskStasus[file] = TASKPROCESSING
			reply.FileName = file
			return nil
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for _, status := range c.Files {
		if status != TASKDONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) ReduceDone() bool {
	for _, status := range c.ReduceTaskStasus {
		if status != TASKDONE {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

//hash % nReduce to generate immediate files
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	filesMap := map[string]int{}
	for _, file := range files {
		filesMap[file] = TASKREADDY
	}
	mapReduceMap := map[string]int{}
	for i := 0; i < nReduce; i++ {
		tmpFile := fmt.Sprintf("immediate-%d.txt", i)
		mapReduceMap[tmpFile] = TASKREADDY
	}
	c := Coordinator{Files: filesMap, NReduce: nReduce, ReduceTaskStasus: mapReduceMap}
	c.server()
	return &c
}
