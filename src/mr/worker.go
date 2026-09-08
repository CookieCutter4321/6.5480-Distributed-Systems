package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

var mapf func(string, string) []KeyValue
var reducef func(string, []string) string

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	mapf = mapf
	reducef = reducef
	// 1. Grab a Task
	CallExample()
}

func CallExample() {
	var workerId int;
	var partitionId int;
	
	args := Args{}
	reply := Reply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
	workerId = reply.WorkerId
	partitionId = ihash(reply.FileName) % reply.NReduce
	fmt.Printf("File assigned: %s\n | Worker ID assigned: %d\n %d (partition)\n", reply.FileName, workerId, partitionId) //figure out partition id ltr

	// Perform the actual work while keeping in mind what partition we are on.
	// The partition can be obtained by hashing the file we are assigned, which will be in 
	// the intermediate file name.

	// 1. read contents of assigned file

	/*
	targetFile, err := os.open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
		return
	}
	content, err := ioutil.ReadAll(targetFile)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	targetFile.Close()


	// 2. Create the File for us to write to as an intemediary: mr-WorkerId-PartitionId
	iFileName := fmt.Sprintf("mr-%d-%d", workerId, partitionId) 

	iFile, err := os.Create(iFileName)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer iFile.Close()


	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 3. Inform via rpc that the task was completed 

	


	//also, need a feature to handle timeouts (10s)
	*/
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}