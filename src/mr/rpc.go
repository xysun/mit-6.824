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
	TaskContent []string // for map: single filename; for reduce: a list of intermediate file names
	NReduce     int      // TODO: separate a "getMeta" RPC call?
	Err         string   // TODO: make it more elegant?
}

type SubmitTaskRequest struct {
	TaskId string
	Files  []string
}

type SubmitTaskResponse struct {
	Msg string
}

const mapTask = "map"
const reduceTask = "reduce"

// errors

const NoTaskAvailable = "No task currently available"
const AllTasksComplete = "All tasks have completed"

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
