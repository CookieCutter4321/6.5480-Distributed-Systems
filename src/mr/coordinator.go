package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type Task struct {
	FileName string
	Status int
	Assigned int64
}

type ReduceTask struct {
	Status int
	Assigned int64
}

type Coordinator struct {
	mu sync.Mutex
	Tasks []Task
	ReduceTasks []ReduceTask
}


var nReduce int
var amountDone int
var reduceDone int
var getTaskMutex sync.Mutex

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	getTaskMutex.Lock()
	defer getTaskMutex.Unlock()
	// TODO: add a queue based data structure instead of iterating for a task.
	mapTasks := c.Tasks

	// Mapping tasks are all complete

	
	if amountDone == len(mapTasks) {
		reduceTasks := c.ReduceTasks

		for i := range nReduce {
			c.mu.Lock()
			CurrentTime := time.Now().Unix()
			Status := reduceTasks[i].Status
			AssignedTime := reduceTasks[i].Assigned

			if (Status == 1 && (CurrentTime - AssignedTime <= 10)) || Status == 2   { 
				c.mu.Unlock()
				continue
			}
			
			// 2. Respond with assignment
			reply.HasTasks = true
			reply.MapTaskCount = len(mapTasks)
			reply.IsReduce = true
			reply.PartitionId = i

			reduceTasks[i].Status = 1
			reduceTasks[i].Assigned = CurrentTime
			c.mu.Unlock()
			break
		}
		
		return nil
	}


	for i := range len(mapTasks) {
		c.mu.Lock()
		CurrentTime := time.Now().Unix()
		FileName := mapTasks[i].FileName
		Status := mapTasks[i].Status
		AssignedTime := mapTasks[i].Assigned

		if (Status == 1 && (CurrentTime - AssignedTime <= 10))|| Status == 2   { 
			c.mu.Unlock()
			continue
		}

		reply.HasTasks = true
		reply.MapTaskCount = len(mapTasks)
		reply.NReduce = nReduce
		reply.FileName = FileName
		reply.WorkerId = i
		

		mapTasks[i].Status = 1
		mapTasks[i].Assigned = CurrentTime
		c.mu.Unlock()
		break
	}

	return nil
}

func (c *Coordinator) DidTask(args *FinishedArgs, reply *FinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	CurrentTime := time.Now().Unix()

	if !args.IsReduce { 
		Task := c.Tasks[args.TaskId]

		// Ignore if elapsed
		if CurrentTime - Task.Assigned > 10 {
			fmt.Println("Task is too late and thus ignored")
			return nil
		}
		if Task.Status != 1 {
			return nil
		}
		
		getTaskMutex.Lock()
		defer getTaskMutex.Unlock()
		amountDone++
		c.Tasks[args.TaskId].Status = 2
	} else {
		Task := c.ReduceTasks[args.TaskId]

		if CurrentTime - Task.Assigned > 10 {
			fmt.Println("Task is too late and thus ignored")
			return nil
		}
		if Task.Status != 1 {
			return nil
		}

		getTaskMutex.Lock()
		defer getTaskMutex.Unlock()
		reduceDone++
		c.ReduceTasks[args.TaskId].Status = 2
	}
	return nil
}

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}


// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return reduceDone == nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, NewNReduce int) *Coordinator {
	c := Coordinator{}

	for _, f := range files {
		c.Tasks = append(c.Tasks, Task{
			FileName: f,
			Status: 0, 
			Assigned: 0,
		})
	}
	for _ = range NewNReduce {
		c.ReduceTasks = append(c.ReduceTasks, ReduceTask {
			Status: 0,
			Assigned: 0,
		})
	}

	nReduce = NewNReduce
	c.server(sockname)
	return &c
}
