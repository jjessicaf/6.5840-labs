package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	filename  string
	status    Status
	startTime time.Time
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
	mapTasksMu           sync.Mutex
	reduceTasksMu        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskResponse) error {
	if !c.mapDone {
		if !c.assignTask(c.mapTasks, "map", c.nReduce, reply) {
			return fmt.Errorf("failed to assign map task")
		}
	} else if !c.reduceDone {
		if !c.assignTask(c.reduceTasks, "reduce", len(c.mapTasks), reply) {
			return fmt.Errorf("failed to assign reduce task")
		}
	}

	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneResponse) error {
	var mu *sync.Mutex
	if args.TaskType == "map" {
		mu = &c.mapTasksMu
	} else {
		mu = &c.reduceTasksMu
	}

	mu.Lock()
	defer mu.Unlock()

	i := args.TaskNumber
	if args.TaskType == "map" && 0 <= i && i < len(c.mapTasks) {
		c.mapTasks[i].status = Complete
		c.mapTasksCompleted += 1
		if c.mapTasksCompleted >= len(c.mapTasks) {
			c.mapDone = true
		}
	} else if args.TaskType == "reduce" && 0 <= i && i < len(c.reduceTasks) {
		c.reduceTasks[i].status = Complete
		c.reduceTasksCompleted += 1
		if c.reduceTasksCompleted >= len(c.reduceTasks) {
			c.reduceDone = true
		}
	} else {
		if args.TaskType != "map" && args.TaskType != "reduce" {
			return fmt.Errorf("invalid task type: %s", args.TaskType)
		}
		return fmt.Errorf("invalid task number: %d", args.TaskNumber)
	}

	return nil
}

func (c *Coordinator) assignTask(tasks []Task, taskType string, n int, reply *RequestTaskResponse) bool {
	var mu *sync.Mutex
	if taskType == "map" {
		mu = &c.mapTasksMu
	} else {
		mu = &c.reduceTasksMu
	}

	mu.Lock()
	defer mu.Unlock()

	for i, task := range tasks {
		if task.status == Unassigned {
			reply.Filename = task.filename
			tasks[i].status = Assigned // Modify the original task
			tasks[i].startTime = time.Now()
			reply.TaskType = taskType
			reply.N = n
			reply.TaskNumber = i
			return true
		}
	}
	return false
}

func (c *Coordinator) updateTasks(tasks []Task) {
	for i := range tasks {
		if tasks[i].status == Assigned && time.Since(tasks[i].startTime).Seconds() >= 10.0 {
			tasks[i].status = Unassigned
		}
	}
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
	if !c.mapDone {
		c.mapTasksMu.Lock()
		defer c.mapTasksMu.Unlock()
		c.updateTasks(c.mapTasks)
	} else if !c.reduceDone {
		c.reduceTasksMu.Lock()
		defer c.reduceTasksMu.Unlock()
		c.updateTasks(c.reduceTasks)
	}

	return c.mapDone && c.reduceDone
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
