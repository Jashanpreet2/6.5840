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

// Status of tasks
type TaskStatus int

const (
	Unavailable TaskStatus = iota
	Idle
	InProgress
	Completed
)

// Type of task
type TaskType int

const (
	MapType TaskType = iota
	ReduceType
)

// Map Task
type MapTask struct {
	worker int
	input  string
}

// Reduce Task
type ReduceTask struct {
	worker               int
	partitionsToRead     map[string]interface{}
	inProgressPartitions map[string]interface{}
	completePartitions   map[string]interface{}
}

// Coordinator
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

type GetTaskReply struct {
	taskType TaskType
	input    string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	defer c.l.Unlock()
	for _, task := range c.mapTasks {
		if task.status == Idle {
			GetTaskReply.taskType = MapType
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
		c.reduceTasks[reduceId].partitionsToRead =
			append(c.reduceTasks[reduceId].partitionsToRead, args.partitions[reduceId])
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	if c.reduceTasks[args.taskId].status == InProgress {
		c.reduceTasks[args.taskId].status = Completed
		c.completedReduces += 1
	} else {
		fmt.Printf("Got completed message for reduce task (id: %d) not in progress\n", args.taskId)
	}
	return nil
}

func (c *Coordinator) GetReducePartitions(args *GetReducePartitionsArgs, reply *GetReducePartitionsReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	// Add guard against requesting for tasks which are complete or not started or never to be started
	if task, ok := c.reduceTasks[args.taskId]; !ok {
		fmt.Printf("ERROR: GetReducePartitions on a task that doesn't exist (ID: %d)\n", args.taskId)
	} else if task.status != InProgress {
		fmt.Printf("ERROR: GetReducePartitions on a task that is not in Progress (ID: %d, Status: %d)\n",
			args.taskId, c.reduceTasks[args.taskId].status)
	} else {
		reply.partitions = slices.Clone(task.partitionsToRead)
	}
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
	return c.completedReduces == c.nReduce
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
