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
	taskId     int
	partitions map[int]string
}

type CompleteMapTaskReply struct{}

// CompleteReduceTask
type CompleteReduceTaskArgs struct {
	taskId     int
	outputFile string
}

type CompleteReduceTaskReply struct{}

// GetReducePartitions
type GetReducePartitionsArgs struct {
	taskId int
}

type GetReducePartitionsReply struct {
	partitions []string
}
