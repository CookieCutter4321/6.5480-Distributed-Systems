package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "io/ioutil"

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
func Worker(sockname string, NewMapf func(string, string) []KeyValue,
	NewReducef func(string, []string) string) {

	coordSockName = sockname
	mapf = NewMapf
	reducef = NewReducef
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
	fmt.Printf("File assigned: %s\n | Worker ID assigned: %d\n %d (partition)\n", reply.FileName, workerId, partitionId)

	// 1. read contents of assigned file
	targetFile, err := os.Open(reply.FileName)
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
	// 3. apply the mapper function and save as json to intermediary
	kva := mapf(reply.FileName, string(content))
	enc := json.NewEncoder(iFile)
	for _, kv := range kva { // Iterating over []mr.KeyValue
		err := enc.Encode(&kv)

		if err != nil {
        	return
   		}
	}
	iFile.Close()
	// 4. Inform via rpc that the task was completed 

	finishedArgs := Args{}
	finishedArgs.FileName = iFileName
	finishedReply := Reply{}
	
	ok := call("Coordinator.DoneDask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
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