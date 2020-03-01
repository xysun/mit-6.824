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

// RPC types for mapreduce

type GetTaskRequest struct {
	WorkerId string
}

type GetTaskResponse struct {
	TaskId      string
	TaskType    string
	TaskContent string
	NReduce     int
	Err         string
}

// errors

const NoTaskAvailable = "No task currently available"
const AllTasksComplete = "All tasks have completed"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
