package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Coordinator struct {
	mu sync.Mutex
	Tasks []string // list of filepath strings
	Statuses []int // 0 = unscheduled, 1 = in progress, 2 = done
	WorkerId int // unique id to incr + 1 for each new worker
}


// Grab an available task
func (c * Coordinator) GetTask(args *Args, reply *Reply) error {
	// TODO: add a queue based data structure instead of iterating for a task.
	tasks := c.Tasks
	statuses := c.Statuses

	for i := range len(tasks) {
		FileName := tasks[i]
		Status := statuses[i]

		c.mu.Lock()
		if Status == 1 || Status == 2 {
			c.mu.Unlock()
			continue
		}

		reply.FileName = FileName
		reply.WorkerId = c.WorkerId
		c.WorkerId++
		statuses[i] = 1
		c.mu.Unlock()
		break
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
	ret := true

	// Your code here.
	for _, s := range c.Statuses {
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
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 1. Load the tasks (files) 
	for _, f := range files {
		c.Tasks = append(c.Tasks, f)
		c.Statuses = append(c.Statuses, 0)
	}

	c.server(sockname)
	return &c
}
