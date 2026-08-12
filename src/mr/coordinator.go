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
	partitionsToRead     []string
	inProgressPartitions []string
	completePartitions   []string
}

// Coordinator
type Coordinator struct {
	// Your definitions here.
	l       sync.Mutex // For locking access to coordinator object
	nMap    int        // No. of map tasks
	nReduce int        // No. of reduce tasks

	// Map tasks grouped by status: Idle, In Progress, and Complete
	// Allows finding available tasks in O(1) when a worker requests a task.
	idleMaps       map[int]*MapTask
	inProgressMaps map[int]*MapTask
	completeMaps   map[int]*MapTask

	// Reduce tasks grouped by status: Unavailable, Idle, In Progress, Complete
	unavailableReduces map[int]*ReduceTask
	idleReduces        map[int]*ReduceTask
	inProgressReduces  map[int]*ReduceTask
	completeReduces    map[int]*ReduceTask
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	for taskId, task := range c.idleMaps {
		// Reply
		reply.TaskId = taskId
		reply.TaskType = MapType
		reply.Input = task.input

		// Assign worker
		task.worker = args.WorkerId

		// Move to in progress
		c.inProgressMaps[taskId] = task
		delete(c.idleMaps, taskId)

		return nil
	}

	for taskId, task := range c.idleReduces {
		reply.TaskId = taskId
		reply.TaskType = ReduceType
		for _, partition := range task.partitionsToRead {
			reply.Input = partition
			break
		}

		return nil
	}

	return fmt.Errorf("No task present")
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()

	// Check if not present
	if _, ok := c.inProgressMaps[args.TaskId]; !ok {
		fmt.Printf("Received CompleteMapTask for task not in progress (Id: %d)", args.TaskId)
		return nil
	}

	for taskId, task := range c.unavailableReduces {
		task.partitionsToRead = append(task.partitionsToRead, args.Partitions[taskId])
		c.idleReduces[taskId] = task
		delete(c.unavailableReduces, taskId)
	}

	// Assign partition to relevant tasks
	// for m := range []map[int]*ReduceTask{c.idleReduces, c.inProgressReduces} {
	// 	for task, taskId := range m {

	// 	}
	// }
	for reduceId := range c.nReduce {
		c.unavailableReduces[reduceId].partitionsToRead =
			append(c.unavailableReduces[reduceId].partitionsToRead, args.Partitions[reduceId])
	}

	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	if task, ok := c.inProgressReduces[args.TaskId]; ok {
		// Set all partitions as complete
		task.completePartitions = task.inProgressPartitions
		task.inProgressPartitions = []string{}

		// Move from inProgressReduces to completeReduces
		c.completeReduces[args.TaskId] = task
		delete(c.inProgressReduces, args.TaskId)
	} else {
		panic(fmt.Sprintf("Got completed message for reduce task (id: %d) not in progress\n", args.TaskId))
	}
	return nil
}

func (c *Coordinator) GetReducePartitions(args *GetReducePartitionsArgs, reply *GetReducePartitionsReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	// Add guard against requesting for tasks which are complete or not started or never to be started
	if task, ok := c.inProgressReduces[args.TaskId]; !ok {
		panic(fmt.Sprintf("ERROR: GetReducePartitions on a task that is not in progress (ID: %d)\n", args.TaskId))
	} else {
		reply.Partitions = slices.Clone(task.partitionsToRead)
		task.inProgressPartitions = append(task.inProgressPartitions, task.partitionsToRead...)
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
	return len(c.completeReduces) == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		l:       sync.Mutex{},
		nMap:    len(files),
		nReduce: nReduce,

		idleMaps:       make(map[int]*MapTask, len(files)),
		inProgressMaps: make(map[int]*MapTask, len(files)),
		completeMaps:   make(map[int]*MapTask, len(files)),

		unavailableReduces: make(map[int]*ReduceTask, nReduce),
		idleReduces:        make(map[int]*ReduceTask, nReduce),
		inProgressReduces:  make(map[int]*ReduceTask, nReduce),
		completeReduces:    make(map[int]*ReduceTask, nReduce),
	}

	for i := range files {
		c.idleMaps[i] = &MapTask{
			worker: -1,
			input:  files[i],
		}
	}

	for i := range nReduce {
		c.unavailableReduces[i] = &ReduceTask{
			worker:               -1,
			partitionsToRead:     []string{},
			inProgressPartitions: []string{},
			completePartitions:   []string{},
		}
	}

	c.server(sockname)
	return &c
}
