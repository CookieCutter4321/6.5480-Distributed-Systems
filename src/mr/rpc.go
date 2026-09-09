package mr


/* Ask the coordinator for a task*/ 
type Args struct {
}

/* file name of an unstarted map task */
type Reply struct {
	FileName string
	WorkerId int
	NReduce int
}



type FinishedArgs struct {
	TaskId int
}

type FinishedReply struct {
}