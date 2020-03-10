package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

type ByKey []KeyValue

// for sorting, copied in mrsequential
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

		time.Sleep(time.Duration(2) * time.Second)
	}

}

func handleTask(task GetTaskResponse, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (SubmitTaskRequest, error) {
	switch task.TaskType {
	case mapTask:
		// TODO: raise error if TaskContent size is not 1
		return SubmitTaskRequest{task.TaskId, handleMapTask(task.TaskId, task.TaskContent[0], task.NReduce, mapf)}, nil
	case reduceTask:
		return SubmitTaskRequest{task.TaskId, []string{handleReduceTask(task.TaskId, task.TaskContent, reducef)}}, nil
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
			intermediateFiles[r], _ = ioutil.TempFile("", "mr-*")
		}
		outf, _ := intermediateFiles[r]
		enc := json.NewEncoder(outf)
		enc.Encode(&kv)
	}

	result := []string{}
	for r, fHandle := range intermediateFiles {
		finalName := fmt.Sprintf("mr-%s-%d", taskId, r)
		result = append(result, finalName)
		fHandle.Close()
		os.Rename(fHandle.Name(), finalName)
	}

	return result
}

func handleReduceTask(taskId string, inputFiles []string, reducef func(string, []string) string) string {
	// produce one single output file
	intermediate := []KeyValue{}
	for _, fname := range inputFiles {
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	// for each key, call reducef, write to output
	oname := fmt.Sprintf("mr-out-%s", taskId)
	ofile, _ := ioutil.TempFile("", "mr-out-*")
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)

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
