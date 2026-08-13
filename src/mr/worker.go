package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	for {
		if err := call("Coordinator.GetTask", &args, &reply); err != nil {
			panic(fmt.Sprintf("Coordinator dysfunctional: Coordinator.GetTask(): %v\n", err))
		}
		switch reply.TaskType {
		case ExitType:
			fmt.Printf("Exiting\n")
			return
		case WaitType:
			fmt.Printf("Waiting 1s\n")
			time.Sleep(time.Second)
		case MapType:
			fmt.Printf("Received map task: %v\n", reply)
			Map(mapf, reply.TaskId, reply.Input, reply.NReduce)
		case ReduceType:
			fmt.Printf("Received reduce task: %v\n", reply)
			Reduce(reducef, reply.TaskId, reply.Input, reply.NMap)
		default:
			panic("Unknown task type received from coordinator")
		}
	}

	// uncomment to send the Example RPC to the coordinator.

}

func Map(mapf func(string, string) []KeyValue, taskId int, filename string, nReduce int) {
	fmt.Printf("Mapping task received. ID: %d, file: %v\n", taskId, filename)
	bytes, err := os.ReadFile(filename)
	if err != nil {
		panic(fmt.Sprintf("Error reading file: %v\n", filename))
	}
	// REMOVE THE [:200]
	content := string(bytes[:200])
	kva := mapf(filename, content)

	// Create nReduce partitions
	partitions := make(map[int]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for reduceId := range nReduce {
		format := fmt.Sprintf("*_%d_%d", taskId, reduceId)
		partitions[reduceId], err = os.CreateTemp("/home/jasha/projects/6.5840/tmp", format)
		if err != nil {
			panic(fmt.Errorf("Error creating partition files: %v", err))
		}
		defer partitions[reduceId].Close()
		encs[reduceId] = json.NewEncoder(partitions[reduceId])
	}

	// Write partition
	for _, kv := range kva {
		encs[ihash(kv.Key)%nReduce].Encode(&kv)
	}

	args := CompleteMapTaskArgs{}
	reply := CompleteMapTaskReply{}

	args.TaskId = taskId
	args.Partitions = make(map[int]string, len(partitions))
	for i, partition := range partitions {
		args.Partitions[i], err = filepath.Abs(partition.Name())
		if err != nil {
			panic(fmt.Sprintf("Failed to get abs path name in worker.go Map: %v\n", err))
		}
	}

	if err := call("Coordinator.CompleteMapTask", &args, &reply); err != nil {
		panic(fmt.Sprintf("Error calling Coordinator.CompleteMapTask: %v\n", err))
	}
}

func Reduce(reducef func(string, []string) string, taskId int, filename string, nPartitions int) {
	fmt.Printf("Reduce task received. ID: %d, file: %v\n", taskId, filename)
	partitionsRead := map[string]struct{}{}
	partitionsToRead := map[string]struct{}{filename: {}}
	kva := []KeyValue{}
	readPartition := func(filename string) {
		file, err := os.Open(filename)
		if err != nil {
			panic(fmt.Sprintf("Failed to open intermediate file: %v\n", filename))
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	awaitPartitions := func() {
		args := GetReducePartitionsArgs{}
		reply := GetReducePartitionsReply{}
		args.TaskId = taskId
		err := call("Coordinator.GetReducePartitions", &args, &reply)
		if err != nil {
			panic(err)
		}
		for len(reply.Partitions) == 0 {
			time.Sleep(time.Second)
			err := call("Coordinator.GetReducePartitions", &args, &reply)
			if err != nil {
				panic(err)
			}
		}
		for _, partition := range reply.Partitions {
			if _, ok := partitionsRead[partition]; ok {
				continue
			}
			partitionsToRead[partition] = struct{}{}
		}
		if len(reply.Partitions) == 0 {
			time.Sleep(time.Second)
		}
	}

	for {
		for partition := range partitionsToRead {
			readPartition(partition)
			partitionsRead[partition] = struct{}{}
			delete(partitionsToRead, partition)
		}
		if len(partitionsRead) == nPartitions {
			break
		}
		fmt.Printf("PartitionsToRead: %d, PartitionsRead: %d\n", nPartitions, len(partitionsToRead))
		if len(partitionsToRead) == 0 {
			fmt.Printf("Waiting partitions. nPartitions: %d, partitionsDone: %d\n", nPartitions, len(partitionsToRead))
			awaitPartitions()
		}
	}
	fmt.Printf("Starting sort")

	sort.Sort(ByKey(kva))
	kvalues := map[string][]string{}
	res := map[string]string{}
	for i := 0; i < len(kva); {
		curKey := kva[i].Key
		kvalues[curKey] = []string{}
		j := i
		for ; j < len(kva) && kva[j].Key == curKey; j++ {
			kvalues[curKey] = append(kvalues[curKey], kva[j].Value)
		}
		i = j
	}
	for key, values := range kvalues {
		res[key] = reducef(key, values)
	}

	outputFile, err := os.CreateTemp("/home/jasha/projects/6.5840/tmp", "")
	if err != nil {
		panic(fmt.Sprintf("Failed to create output file: %v\n", err))
	}
	enc := json.NewEncoder(outputFile)
	if err := enc.Encode(res); err != nil {
		panic("Failed to write to reduce output file")
	}
	defer outputFile.Close()
	fmt.Printf("Reduce completed, output: %v\n", outputFile)
	args := CompleteReduceTaskArgs{}
	args.TaskId = taskId
	args.OutputFile = outputFile.Name()
	reply := CompleteReduceTaskReply{}
	err = call("Coordinator.CompleteReduceTask", &args, &reply)
	if err != nil {
		panic(fmt.Sprintf("Error calling Coordinator.CompleteReduceTask: %v\n", err))
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err != nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		log.Printf("%d: call failed err %v", os.Getpid(), err)
		return err
	}
	return nil
}
