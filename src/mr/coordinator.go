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

type TaskState int
type TaskType int
type JobStage int

const (
	NotStarted TaskState = iota
	Running
	Finished
)

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	NoTask // No task available
)

const (
	MapStage JobStage = iota
	ReduceStage
	ExitStage
)

type Task struct {
	Type      TaskType
	State     TaskState
	StartTime time.Time
	File      string
	WorkerID  int
	TaskId    int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMap        int
	stage       JobStage
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapTask {
		if c.mapTasks[args.TaskId].State == Running && c.mapTasks[args.TaskId].WorkerID != -1 && c.mapTasks[args.TaskId].WorkerID == args.WorkerID {
			c.mapTasks[args.TaskId].State = Finished
			c.mapTasks[args.TaskId].WorkerID = -1
			c.nMap -= 1
			fmt.Printf("Map task %v finished, nMap: %v, nReduce: %v\n", args.TaskId, c.nMap, c.nReduce)
		}
	} else if args.TaskType == ReduceTask {
		if c.reduceTasks[args.TaskId].State == Running && c.reduceTasks[args.TaskId].WorkerID != -1 && c.reduceTasks[args.TaskId].WorkerID == args.WorkerID {
			c.reduceTasks[args.TaskId].State = Finished
			c.reduceTasks[args.TaskId].WorkerID = -1
			c.nReduce -= 1
			fmt.Printf("Reduce task %v finished, nMap: %v, nReduce: %v\n", args.TaskId, c.nMap, c.nReduce)
		}
	}

	if c.nMap == 0 && c.nReduce == 0 {
		// fmt.Printf("All tasks finished\n")
		c.stage = ExitStage
		reply.CanExit = true
	} else if c.nMap == 0 {
		c.stage = ReduceStage
		reply.CanExit = false
	}
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stage == ExitStage {
		reply.TaskType = ExitTask
		return nil
	}
	if c.stage == MapStage {
		for i, task := range c.mapTasks {
			if task.State == NotStarted {
				c.mapTasks[i].State = Running
				c.mapTasks[i].StartTime = time.Now()
				c.mapTasks[i].WorkerID = args.WorkerID
				reply.TaskType = MapTask
				reply.File = task.File
				reply.TaskId = task.TaskId
				// fmt.Printf("Assign map task %v to worker %v, filename:%s\n", task.TaskId, args.WorkerID, reply.File)
				go c.waitTask(&c.mapTasks[i])
				return nil
			}
		}
	}
	if c.stage == ReduceStage {
		for i, task := range c.reduceTasks {
			if task.State == NotStarted {
				c.reduceTasks[i].State = Running
				c.reduceTasks[i].StartTime = time.Now()
				c.reduceTasks[i].WorkerID = args.WorkerID
				reply.TaskType = ReduceTask
				reply.TaskId = task.TaskId
				reply.File = task.File
				go c.waitTask(&c.reduceTasks[i])
				return nil
			}
		}
	}
	reply.TaskType = NoTask
	return nil
}

func (c *Coordinator) waitTask(task *Task) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.State == Running {
		if task.Type == MapTask {
			fmt.Printf("Map task %v timeout, reassigning\n", task.TaskId)
			c.mapTasks[task.TaskId].State = NotStarted
			c.mapTasks[task.TaskId].WorkerID = -1
			// c.nMap += 1
		} else {
			fmt.Printf("Reduce task %v timeout, reassigning\n", task.TaskId)
			c.reduceTasks[task.TaskId].State = NotStarted
			c.reduceTasks[task.TaskId].WorkerID = -1
			// c.nReduce += 1
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stage == ExitStage {
		if c.nMap == 0 && c.nReduce == 0 {
			ret = true
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapTasks = make([]Task, c.nMap)
	c.reduceTasks = make([]Task, c.nReduce)
	for i, file := range files {
		c.mapTasks[i] = Task{Type: MapTask, State: NotStarted, File: file, TaskId: i}
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = Task{Type: ReduceTask, State: NotStarted, File: "", TaskId: i}
	}
	c.stage = MapStage

	// fmt.Printf("Coordinator created, nReduce: %v, nMap: %v\n", c.nReduce, c.nMap)

	os.Mkdir(TempDir, 0755)

	c.server()
	return &c
}
