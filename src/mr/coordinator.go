package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Task struct {
	filename  string
	status    Status
	startTime int
}

type Status int

const (
	Unassigned Status = iota // 0
	Assigned                 // 1
	Complete                 // 2
)

type Coordinator struct {
	mapTasks             []Task
	reduceTasks          []Task
	nReduce              int
	mapTasksCompleted    int
	reduceTasksCompleted int
	mapDone              bool
	reduceDone           bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskResponse) error {
	// Go through files and find a filename that hasn't been assigned yet
	if !c.mapDone {
		for i, task := range c.mapTasks {
			if task.status == 0 {
				reply.Filename = task.filename
				task.status = Assigned
				reply.TaskType = "map"
				reply.N = c.nReduce
				reply.TaskNumber = i
				return nil
			} else if task.status == 1 {
				// Has it taken longer than 10s?
			} else if task.status == 2 {
				c.mapTasksCompleted += 1
			}
		}
		if c.mapTasksCompleted >= len(c.mapTasks) {
			c.mapDone = true
		}
	} else if !c.reduceDone {
		for i, task := range c.reduceTasks {
			if task.status == 0 {
				reply.Filename = task.filename
				task.status = Assigned
				reply.TaskType = "reduce"
				reply.N = len(c.mapTasks)
				reply.TaskNumber = i
				return nil
			} else if task.status == 1 {
				// Has it taken longer than 10s?
			} else if task.status == 2 {
				c.reduceTasksCompleted += 1
			}
		}
		if c.reduceTasksCompleted >= len(c.reduceTasks) {
			c.reduceDone = true
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// go through map tasks and check if they are done and how much time has elapsed

	// go through reduce tasks and check if the yare done and how much time has elapsed

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.mapTasks = make([]Task, len(files))
	for i, filename := range files {
		c.mapTasks[i].filename = filename
	}

	c.server()
	return &c
}
