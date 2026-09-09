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
	var nReduce int;

	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
	workerId = reply.WorkerId
	nReduce = reply.NReduce
	fmt.Printf("File assigned: %s\n | worker ID assigned: %d\n", reply.FileName, workerId)

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

	// 3. apply the mapper function and save as json to intermediary

	kva := mapf(reply.FileName, string(content))

	buckets := make([][]KeyValue, nReduce) 


	// Figure out partitioning in memory first before I/O ops
	for _, kv := range kva {
		partitionId := ihash(kv.Key) % nReduce
		buckets[partitionId] = append(buckets[partitionId], kv)
	}

	for p, arr := range buckets {

		iFileName := fmt.Sprintf("mr-%d-%d", workerId, p) 
		iFile, err := os.OpenFile(iFileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			fmt.Printf("Error creating file: %v\n", err)
			return
		}
		defer iFile.Close()

		enc := json.NewEncoder(iFile)
		for _, kv := range arr {
			err = enc.Encode(&kv)

			if err != nil {
				return
			}
		}
	}

	// 4. Inform via rpc that the task was completed 
	/*
	finishedArgs := FinishedArgs{}
	finishedArgs.TaskId = workerId
	finishedReply := FinishedReply{}
	
	ok = call("Coordinator.DoneDask", &finishedArgs, &finishedReply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}*/

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