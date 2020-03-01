package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const unassigned string = "unassigned"
const completed string = "completed"

type Assigned struct {
	WorkerId     string
	AssignedTime time.Time
}

type Task struct {
	Id      string
	Type    string      // "map" or "reduce" TODO: enum
	Content string      // for map: it's input file name; for reduce it's `R`
	Status  interface{} // "unassigned/completed" or Assigned
}

func (task Task) isCompleted() bool {
	s, ok := task.Status.(string)
	if ok {
		return s == completed
	} else {
		return false
	}
}

func (task Task) toGetTaskResponse(response *GetTaskResponse) {
	response.TaskId = task.Id
	response.TaskType = task.Type
	response.TaskContent = task.Content
}

type Master struct {
	// Your definitions here.
	MapTasks    map[string]*Task
	ReduceTasks map[string]*Task
	nReduce     int
}

func updateStatus(m map[string]*Task) bool {
	// return whether everything is completed
	completed := true
	now := time.Now()
	var tolerance float64 = 5

	for k, v := range m {
		task := *v

		completed = completed && task.isCompleted()
		// check if an Assigned date is more than 10 seconds old,
		fmt.Println("%s %T", v.Id, task.Status)
		t, ok := v.Status.(Assigned)
		if ok {
			if now.Sub(t.AssignedTime).Seconds() > tolerance {
				fmt.Printf("Task %s is more than %.f seconds old, assigned time %s\n", k, tolerance, t.AssignedTime)
				m[k] = &Task{task.Id, task.Type, task.Content, unassigned}
			}
		}
	}

	return completed

}

// map collection functions
func find(m map[string]*Task, f func(Task) bool) (*Task, error) {
	for _, v := range m {
		task := *v
		if f(task) {
			return v, nil
		}
	}
	return &Task{}, errors.New("No applicable task found")
}

func all(m map[string]*Task, f func(Task) bool) bool {
	result := true
	for _, v := range m {
		result = result && f(*v)
	}
	return result
}

func isStatus(task Task, status string) bool {
	s, ok := task.Status.(string)
	return ok && s == status
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	workerId := args.WorkerId
	fmt.Printf("Got GetTask request from worker %s\n", workerId)
	now := time.Now()
	unassignedF := func(task Task) bool {
		return isStatus(task, unassigned)
	}

	completedF := func(task Task) bool {
		return isStatus(task, completed)
	}

	if all(m.MapTasks, completedF) && all(m.ReduceTasks, completedF) {
		reply.Err = AllTasksComplete
		return nil
	}

	mapTask, err := find(m.MapTasks, unassignedF)
	if err != nil {
		// only proceed to reduce tasks if all map tasks are completed
		if all(m.MapTasks, func(task Task) bool { return task.Status == completed }) {
			reduceTask, err2 := find(m.ReduceTasks, unassignedF)
			if err2 != nil {
				reply.Err = NoTaskAvailable
			} else {
				reduceTask.toGetTaskResponse(reply)
				reply.NReduce = m.nReduce
				reduceTask.Status = Assigned{workerId, now}
			}

		} else {
			reply.Err = NoTaskAvailable
		}

	} else {
		mapTask.toGetTaskResponse(reply)
		reply.NReduce = m.nReduce
		mapTask.Status = Assigned{workerId, now}
	}

	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Done is called every second, so we can use this to check the task status and reassign
	// go through every task in MapTasks and ReduceTasks

	ret := updateStatus(m.MapTasks) && updateStatus(m.ReduceTasks)

	fmt.Printf("Done checking result %b\n", ret)

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// create map tasks
	m.MapTasks = map[string]*Task{}
	m.ReduceTasks = map[string]*Task{}
	m.nReduce = nReduce

	for i, fname := range files {
		taskId := fmt.Sprintf("map%d", i)
		m.MapTasks[taskId] = &Task{taskId, "map", fname, unassigned}
	}

	for i := 0; i < nReduce; i++ {
		taskId := fmt.Sprintf("reduce%d", i)
		m.ReduceTasks[taskId] = &Task{taskId, "reduce", string(i), unassigned}
	}

	fmt.Printf("Master initialized, map tasks total %d, reduce tasks total %d\n", len(m.MapTasks), len(m.ReduceTasks))

	// Your code here.

	m.server()
	return &m
}
