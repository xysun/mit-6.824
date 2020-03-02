package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

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
	for {
		fmt.Println("Asking for task...")
		task, err := getTask(workerId)
		if err == nil {
			switch task.Err {
			case "":
				fmt.Printf("Got new task Id %s, type %s, content %s, nreduce %d\n", task.TaskId, task.TaskType, task.TaskContent, task.NReduce)
				taskSubmitRequest, err := handleTask(task, mapf, reducef)
				if err == nil {
					fmt.Println("Task handled!")
					call("Master.SubmitTask", &taskSubmitRequest, &SubmitTaskResponse{})
				}

			case NoTaskAvailable:
				fmt.Println(NoTaskAvailable)
			case AllTasksComplete:
				fmt.Println("No more tasks, exiting")
				return
			}
		} else {
			fmt.Println("server error", err.Error())
		}

		time.Sleep(time.Duration(3) * time.Second)
	}

	// return

}

func handleTask(task GetTaskResponse, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (SubmitTaskRequest, error) {
	switch task.TaskType {
	case mapTask:
		// TODO: raise error if TaskContent size is not 1
		return SubmitTaskRequest{task.TaskId, handleMapTask(task.TaskId, task.TaskContent[0], task.NReduce, mapf)}, nil
	case reduceTask:
		return SubmitTaskRequest{task.TaskId, []string{handleReduceTask(task.TaskId, task.TaskContent)}}, nil
	default:
		return SubmitTaskRequest{}, errors.New(fmt.Sprintf("Invalid task type %s", task.TaskType))
	}
}

func handleMapTask(taskId string, inputFile string, nReduce int, mapf func(string, string) []KeyValue) []string {
	// call mapF, hash the key-value pairs and
	// produce mr-mapTaskId-R files, R from 0 to NReduce
	// basically copy from mrsequential
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	file.Close()
	kva := mapf(inputFile, string(content))
	var intermediateFiles = make(map[int]*os.File)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		_, ok := intermediateFiles[r]
		if !ok {
			intermediateFiles[r], _ = os.Create(fmt.Sprintf("mr-%s-%d", taskId, r))
		}
		outf, _ := intermediateFiles[r]
		fmt.Fprintf(outf, "%v %v\n", kv.Key, kv.Value)
	}

	result := []string{}
	for r, fHandle := range intermediateFiles {
		result = append(result, fmt.Sprintf("mr-%s-%d", taskId, r))
		fHandle.Close()
	}

	return result
}

func handleReduceTask(taskId string, inputFiles []string) string {
	// produce one single output file
	return ""
}

func getTask(workerId string) (GetTaskResponse, error) {
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
