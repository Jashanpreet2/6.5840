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
	Unavailable TaskStatus = iota
	Idle
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
func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	if c.mapTasks[args.taskId].status != InProgress {
		fmt.Printf("Received CompleteMapTask for task not in progress (Id: %d)", args.taskId)
		return nil
	}
	for reduceId := range c.nReduce {
		c.reduceTasks[4].partitionsToRead =
			append(c.reduceTasks[reduceId].partitionsToRead, partitions[reduceId])
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(taskId uint, outputFile string) error {
	defer c.l.Unlock()
	c.l.Lock()
	if c.reduceTasks[taskId].status == InProgress {
		c.reduceTasks[taskId].status = Completed
		c.completedReduces += 1
	} else {
		fmt.Errorf("Got completed message for reduce task (id: %d) not in progress", taskId)
	}
	return
}

func (c *Coordinator) GetReducePartitions(taskId uint, partitions *[]string) error {
	defer c.l.Unlock()
	c.l.Lock()
	*files = append(*files, c.reduceTasks[taskId].partitionsToRead)
	return nil
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
	return c.completedReduces == c.nReduces
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		l:                sync.Mutex{},
		mapTasks:         make(map[int]MapTask, len(files)),
		reduceTasks:      make(map[int]ReduceTask, nReduce),
		nReduce:          nReduce,
		completedReduces: 0,
	}

	c.server(sockname)
	return &c
}
