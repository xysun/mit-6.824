package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// assign a worker uuid

	workerId := uuid.New().String()
	fmt.Println("Worker", workerId, "started")

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	GetTask(workerId)

}

func GetTask(workerId string) {
	args := GetTaskRequest{workerId}
	reply := GetTaskResponse{}
	call("Master.GetTask", &args, &reply)
	fmt.Printf("reply Id %s, type %s, content %s\n", reply.TaskId, reply.TaskType, reply.TaskContent)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
