package mr


/* Ask the coordinator for a task*/ 
type Args struct {
}

/* file name of an unstarted map task */
type Reply struct {
	FileName string
	WorkerId int
	NReduce int

	MapTaskCount int
	IsReduce bool
	PartitionId int // only for reduce tasks. following mr-*-PartitionId
	HasTasks bool
}



type FinishedArgs struct {
	TaskId int
	IsReduce bool
}

type FinishedReply struct {
}