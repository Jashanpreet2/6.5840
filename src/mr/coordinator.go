package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
)

type TaskStatus int

const (
	Unavailable TaskStatus = iota
	Idle
	InProgress
	Completed
)

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
)

type MapTask struct {
	worker int
	status TaskStatus
	input  string
}

type ReduceTask struct {
	worker           int
	status           TaskStatus
	partitionsToRead []string
}

type Coordinator struct {
	// Your definitions here.
	l                sync.Mutex
	mapTasks         map[int]*MapTask
	reduceTasks      map[int]*ReduceTask
	nReduce          int
	completedReduces int
}

type GetTaskArgs struct {
	workerId string
}

type GetTaskReply[T MapTask | ReduceTask] struct {
	task T
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask() error {
	defer c.l.Unlock()
	for _, task := range c.mapTasks {
		if task.status == Idle {

		}
	}
	return nil
}

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
		mapTasks:         make(map[int]*MapTask, len(files)),
		reduceTasks:      make(map[int]*ReduceTask, nReduce),
		nReduce:          nReduce,
		completedReduces: 0,
	}

	for i := range files {
		c.mapTasks[i] = &MapTask{
			worker: -1,
			status: Idle,
			input:  files[i],
		}
	}

	for i := range nReduce {
		c.reduceTasks[i] = &ReduceTask{
			status:           Unavailable,
			partitionsToRead: []string{},
		}
	}

	c.server(sockname)
	return &c
}
