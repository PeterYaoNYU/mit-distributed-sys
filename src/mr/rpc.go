package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ReportTaskDoneArgs struct {
	TaskType TaskType
	TaskId   int
}

type ReportTaskDoneReply struct {
	canExit bool
}

type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskId   int
	File     string
	nReduce  int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
