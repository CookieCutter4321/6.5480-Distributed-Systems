package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "io/ioutil"
import "sort"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

var mapf func(string, string) []KeyValue
var reducef func(string, []string) string

// main/mrworker.go calls this function.
func Worker(sockname string, NewMapf func(string, string) []KeyValue,
	NewReducef func(string, []string) string) {

	coordSockName = sockname
	mapf = NewMapf
	reducef = NewReducef
	// 1. Grab a Task

	for {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			return
		}
		if reply.HasTasks == false {
			continue
		}

		if reply.IsReduce == false {
			mapTask(reply)
		} else{
			reduceTask(reply)
		}
	}
}

func mapTask(reply Reply) {

	workerId := reply.WorkerId
	nReduce := reply.NReduce
	//fmt.Printf("File assigned: %s | worker ID assigned: %d\n", reply.FileName, workerId)

	// 1. read contents of assigned file
	targetFile, err := os.Open(reply.FileName)
	if err != nil {
		return
	}
	content, err := ioutil.ReadAll(targetFile)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
		return
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

		enc := json.NewEncoder(iFile)
		for _, kv := range arr {
			err = enc.Encode(&kv)

			if err != nil {
				return
			}
		}
		iFile.Close()
	}

	//fmt.Printf("worker ID finished: %d\n", workerId)
	// 4. Inform via rpc that the task was completed 
	finishedArgs := FinishedArgs{}
	finishedArgs.TaskId = workerId
	finishedReply := FinishedReply{}
	
	ok := call("Coordinator.DidTask", &finishedArgs, &finishedReply)
	if !ok {
		return
	}
}

func reduceTask(reply Reply) {
	// Grab all relevant files based on mr-*-PartitionId
	partitionId := reply.PartitionId
	mapTaskCount := reply.MapTaskCount

	var intermediate []KeyValue
	//fmt.Printf("Starting reduce for partition %d \n", partitionId)
	for i := range mapTaskCount {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, partitionId))
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	// sort before
	sort.Sort(ByKey(intermediate))

	i := 0
	oname := fmt.Sprintf("mr-out-%d", partitionId)
	ofile, _ := os.Create(oname)
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	finishedArgs := FinishedArgs{}
	finishedArgs.TaskId = partitionId
	finishedArgs.IsReduce = true
	finishedReply := FinishedReply{}
	
	//fmt.Printf("Finished reduce for partition %d \n", partitionId)
	ok := call("Coordinator.DidTask", &finishedArgs, &finishedReply)
	if !ok {
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
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}