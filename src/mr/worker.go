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


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// 1. Grab a Task
	CallExample()
}

func CallExample() {
	args := Args{}
	reply := Reply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
	fmt.Printf("File assigned: %s\n | Worker ID assigned: %d\n", reply.FileName, reply.WorkerId)


	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// Perform the actual work while keeping in mind what partition we are on.
	// The partition can be obtained by hashing the file we are assigned, which will be in 
	// the intermediate file name.

	// 1. Create the File for us to write to.

	// 2. Pass to write.

	// 3. Inform via rpc that the task was completed 
	
	//also, need a feature to handle timeouts (10s)
	


	
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
