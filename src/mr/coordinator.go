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
type Coordinator struct {
	mu sync.Mutex
	Tasks []Task
}


var nReduce int
func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	// TODO: add a queue based data structure instead of iterating for a task.
	tasks := c.Tasks

	for i := range len(tasks) {
		c.mu.Lock()

		FileName := tasks[i].FileName
		Status := tasks[i].Status
		AssignedTime := tasks[i].Assigned
		CurrentTime := time.Now().Unix()


		if (Status == 1 && (CurrentTime - AssignedTime <= 10))|| Status == 2   { // cont if alrdy in prog / done, and if within the 10s limit
			c.mu.Unlock()
			continue
		}
		fmt.Printf("Reassigning task %d with Status %d, Elapsed time since last assignment: %d\n", i, Status, CurrentTime - AssignedTime)
		
		reply.NReduce = nReduce
		reply.FileName = FileName
		reply.WorkerId = i
		
		tasks[i].Status = 1
		tasks[i].Assigned = CurrentTime
		c.mu.Unlock()
		break
	}
	return nil
}

func (c *Coordinator) DidTask(args *FinishedArgs, reply *FinishedReply) error {
	CurrentTime := time.Now().Unix()
	Task := c.Tasks[args.TaskId]

	// Ignore if elapsed
	if CurrentTime - Task.Assigned > 10 {
		fmt.Println("Task is too late and thus ignored")
		return nil
	}

	c.Tasks[args.TaskId].Status = 2
	fmt.Printf("%d done", args.TaskId)
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
	ret := true

	// Your code here.
	for _, t := range c.Tasks {
		s := t.Status
		if s == 0 || s == 1 {
			ret = false
			break
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, NewNReduce int) *Coordinator {
	c := Coordinator{}

	// 1. Load the tasks (files) 
	for _, f := range files {
		c.Tasks = append(c.Tasks, Task{
			FileName: f,
			Status: 0, 
			Assigned: 0, // since unix epoch?
		})
	}

	nReduce = NewNReduce
	c.server(sockname)
	return &c
}
