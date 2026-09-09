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
	FileName string // Inform the coordinator the relevant file to grab
}

type FinishedReply struct {
}