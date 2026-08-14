package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"
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
	WaitType
	ExitType
)

// Map Task
type MapTask struct {
	worker int
	input  string
	t      *time.Timer
}

// Reduce Task
type ReduceTask struct {
	partitions []string
	t          *time.Timer
}

// Coordinator
type Coordinator struct {
	// Your definitions here.
	l       sync.Mutex // For locking access to coordinator object
	nMap    int        // No. of map tasks
	nReduce int        // No. of reduce tasks
	outDir  string

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

func freeTask(c *Coordinator, taskType TaskType, taskId int) {
	defer c.l.Unlock()
	c.l.Lock()
	name := map[TaskType]string{MapType: "Map", ReduceType: "Reduce"}[taskType]
	fmt.Printf("Freeing task %d of type %v\n", taskId, name)
	switch taskType {
	case MapType:
		if _, ok := c.inProgressMaps[taskId]; !ok {
			return
		}
		c.inProgressMaps[taskId].t = nil
		c.idleMaps[taskId] = c.inProgressMaps[taskId]
		delete(c.inProgressMaps, taskId)
	case ReduceType:
		if _, ok := c.inProgressReduces[taskId]; !ok {
			return
		}
		c.inProgressReduces[taskId].t = nil
		c.idleReduces[taskId] = c.inProgressReduces[taskId]
		delete(c.inProgressReduces, taskId)
	default:
		log.Panicf("Invalid task type receieved in freeTask: %d", taskType)
	}
}

// taskId must be in c.inProgress{Reduces, Maps}. Must lock before starting
func startTimer(c *Coordinator, taskType TaskType, taskId int) {
	name := map[TaskType]string{MapType: "Map", ReduceType: "Reduce"}[taskType]
	fmt.Printf("Started timer for %s task %d\n", name, taskId)
	switch taskType {
	case MapType:
		c.inProgressMaps[taskId].t = time.AfterFunc(10*time.Second, func() {
			freeTask(c, taskType, taskId)
		})
	case ReduceType:
		c.inProgressReduces[taskId].t = time.AfterFunc(10*time.Second, func() {
			freeTask(c, taskType, taskId)
		})
	default:
		log.Panicf("Invalid task type receieved in freeTask: %d", taskType)
	}
	showState(c)
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	for taskId, task := range c.idleMaps {
		// Reply
		reply.TaskId = taskId
		reply.TaskType = MapType
		reply.Input = task.input

		// Move to in progress
		c.inProgressMaps[taskId] = task

		delete(c.idleMaps, taskId)

		// Start timer to free this task
		startTimer(c, reply.TaskType, reply.TaskId)

		return nil
	}

	for taskId, task := range c.idleReduces {
		reply.TaskId = taskId
		reply.TaskType = ReduceType
		for _, partition := range task.partitions {
			reply.Input = partition
			break
		}
		c.inProgressReduces[taskId] = task
		delete(c.idleReduces, taskId)

		// Start timer to free task
		// here
		startTimer(c, reply.TaskType, reply.TaskId)

		return nil
	}

	// Return Exit if all tasks complete
	if len(c.completeReduces) == c.nReduce {
		reply.TaskType = ExitType
		reply.TaskId = -1
		reply.Input = ""

		return nil
	}

	// No task available but overall job still in progress, tell worker to stay on standby
	reply.TaskType = WaitType
	reply.TaskId = -1
	reply.Input = ""
	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	fmt.Println("Completing map task")
	// Move all reduces from unavailable if there are any
	if len(c.unavailableReduces) > 0 {
		c.idleReduces = c.unavailableReduces
		c.unavailableReduces = map[int]*ReduceTask{}
	}

	// Assert that map is in progress
	if _, ok := c.inProgressMaps[args.TaskId]; !ok {
		fmt.Printf("Received CompleteMapTask for task not in progress (Id: %d)", args.TaskId)
		return fmt.Errorf("Map task is already complete")
	}

	// Stop timer
	c.inProgressMaps[args.TaskId].t.Stop()
	c.inProgressMaps[args.TaskId].t = nil
	c.completeMaps[args.TaskId] = c.inProgressMaps[args.TaskId]
	delete(c.inProgressMaps, args.TaskId)

	// Store partitions
	for taskId, task := range c.unavailableReduces {
		task.partitions = append(task.partitions, args.Partitions[taskId])
		c.idleReduces[taskId] = task
		delete(c.unavailableReduces, taskId)
	}

	// Assign partition to relevant tasks
	for _, m := range []map[int]*ReduceTask{c.idleReduces, c.inProgressReduces} {
		for taskId, task := range m {
			task.partitions = append(task.partitions, args.Partitions[taskId])
		}
	}

	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	defer c.l.Unlock()
	c.l.Lock()
	showState(c)
	if task, ok := c.inProgressReduces[args.TaskId]; ok {
		// Move from inProgressReduces to completeReduces
		c.inProgressReduces[args.TaskId].t.Stop()
		c.inProgressReduces[args.TaskId].t = nil
		c.completeReduces[args.TaskId] = task
		newFileName := fmt.Sprintf("mr-out-%d", args.TaskId)

		path := filepath.Join(filepath.Dir(args.OutputFile), newFileName)
		os.Rename(args.OutputFile, path)

		// TEMP Test for correctness
		absPath, err := filepath.Abs(path)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Wrote file: %v", absPath)

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
	if task, ok := c.inProgressReduces[args.TaskId]; ok {
		reply.Partitions = slices.Clone(task.partitions)
		c.inProgressReduces[args.TaskId].t.Reset(10 * time.Second)
		return nil
	} else {
		return fmt.Errorf("This reduce task is not in progress")
	}
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

func keys(m map[int]*ReduceTask) []int {
	ks := make([]int, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	return ks
}
func showState(c *Coordinator) {
	for taskId, task := range c.inProgressReduces {
		hasTimer := task.t != nil
		fmt.Printf("Reduce task %d has timer? %v\n", taskId, hasTimer)
	}
	fmt.Printf("reduce keys: unavailable=%v idle=%v in-progress=%v complete=%v\n",
		keys(c.unavailableReduces), keys(c.idleReduces), keys(c.inProgressReduces), keys(c.completeReduces))
	fmt.Printf("maps (%d total):\n  idle: %d\n  in-progress: %d\n  complete: %d\nreduces (%d total):\n  unavailable: %d\n  idle: %d\n  in-progress: %d\n  complete: %d\n",
		c.nMap, len(c.idleMaps), len(c.inProgressMaps), len(c.completeMaps),
		c.nReduce, len(c.unavailableReduces), len(c.idleReduces), len(c.inProgressReduces), len(c.completeReduces))
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		l:              sync.Mutex{},
		nMap:           len(files),
		nReduce:        nReduce,
		outDir:         filepath.Dir(files[0]),
		idleMaps:       make(map[int]*MapTask, len(files)),
		inProgressMaps: make(map[int]*MapTask, len(files)),
		completeMaps:   make(map[int]*MapTask, len(files)),

		unavailableReduces: make(map[int]*ReduceTask, nReduce),
		idleReduces:        make(map[int]*ReduceTask, nReduce),
		inProgressReduces:  make(map[int]*ReduceTask, nReduce),
		completeReduces:    make(map[int]*ReduceTask, nReduce),
	}
	fmt.Printf("First file's dir: %v, dir of tmp.txt: %v\n", filepath.Dir(files[0]), filepath.Dir("tmp.txt"))
	for i := range files {
		c.idleMaps[i] = &MapTask{
			worker: -1,
			input:  files[i],
		}
	}

	for i := range nReduce {
		c.unavailableReduces[i] = &ReduceTask{
			partitions: []string{},
		}
	}

	monitor := func() {
		defer c.l.Unlock()
		for {
			time.Sleep(6 * time.Second)
			c.l.Lock()
			showState(&c)
			c.l.Unlock()
		}
	}
	go monitor()
	c.server(sockname)
	return &c
}
