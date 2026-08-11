package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type MapTask struct {
	id     int
	status TaskStatus
	input  string
	output string
}

type ReduceTask struct {
	id         int
	status     TaskStatus
	inputFiles []string
	output     string
}

type Coordinator struct {
	// Your definitions here.
	l           sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nReduce     int
	done        bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CompleteMapTask(task MapTask) {
}

func (c *Coordinator) CompleteReduceTask(task MapTask) {
	c.l.Lock()
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	defer c.l.Unlock()
	c.l.Lock()

	// c.done stores whether the task is done. It is set to true when all of
	// the reduce tasks are done. The logic is handled in CompleteReduceTask
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server(sockname)
	return &c
}
