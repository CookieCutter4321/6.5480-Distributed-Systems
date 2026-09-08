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



/* Ask the coordinator for a task*/ 
type Args struct {
}

/* file name of an unstarted map task */
type Reply struct {
	FileName string
	WorkerId int
}