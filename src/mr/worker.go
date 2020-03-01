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

	// TODO: add time.sleep if no task
	task, err := GetTask(workerId)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("reply Id %s, type %s, content %s, nreduce %d\n", task.TaskId, task.TaskType, task.TaskContent, task.NReduce)
	// TODO if task type is map: produce mr-mapTaskId-R files, R from 0 to NReduce

	// TODO: if task type is reduce, ask master for all intermediate file names, sort then produce final file

	// TODO submit task
	return

}

func GetTask(workerId string) (GetTaskResponse, error) {
	args := GetTaskRequest{workerId}
	reply := GetTaskResponse{}
	err := call("Master.GetTask", &args, &reply)
	return reply, err

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)

}
