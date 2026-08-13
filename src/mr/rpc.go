package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// CompleteMapTask
type CompleteMapTaskArgs struct {
	TaskId     int
	Partitions map[int]string
}

type CompleteMapTaskReply struct{}

// CompleteReduceTask
type CompleteReduceTaskArgs struct {
	TaskId     int
	OutputFile string
}

type CompleteReduceTaskReply struct{}

// GetReducePartitions
type GetReducePartitionsArgs struct {
	TaskId int
}

type GetReducePartitionsReply struct {
	Partitions []string
}

// GetTask
type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskId   int
	TaskType TaskType
	Input    string
	NReduce  int
	NMap     int
}
