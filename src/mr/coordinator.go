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

type JobStatus int

const (
	// Coordinator timeouts
	TaskTimeout       = 10 * time.Second
	HeartbeatInterval = 1 * time.Second

	JobStatusPending JobStatus = iota
	JobStatusMap
	JobStatusReduce
	JobStatusFinished
)

type MapTask struct {
	WorkerId int
	TaskStatus
	InputFilePath string

	TaskStartTime time.Time
}

type ReduceTask struct {
	WorkerId int
	TaskStatus
	TaskStartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	JobStatus

	MapperNumber  int
	ReducerNumber int

	// Task tracker
	MapTaskTracker            []*MapTask
	MapTaskUnfinishedCount    int
	ReduceTaskTracker         []*ReduceTask
	ReduceTaskUnfinishedCount int

	mu sync.Mutex // Single lock for all coordinator state

}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(req *TaskRequest, res *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	res.MapperNumber = c.MapperNumber
	res.ReducerNumber = c.ReducerNumber

	// Check job status
	if c.JobStatus == JobStatusPending {
		res.TaskType = TaskTypeWait
		return nil
	}

	if c.JobStatus == JobStatusFinished {
		res.TaskType = TaskTypeFinish
		return nil
	}

	if c.JobStatus == JobStatusMap && c.MapTaskUnfinishedCount > 0 { // Assign work
		res.TaskType = TaskTypeMap

		// Read what Map tasks are not being assigned
		for i, mapTask := range c.MapTaskTracker {
			if mapTask.TaskStatus == TaskStatusPending {
				mapTask.TaskStatus = TaskStatusRunning
				mapTask.TaskStartTime = time.Now()
				res.TaskId = i
				res.TaskInputFileName = string(mapTask.InputFilePath)
				return nil
			}
		}

		// All tasks are running by other workers
		res.TaskType = TaskTypeWait
		return nil

	}

	if c.JobStatus == JobStatusReduce && c.ReduceTaskUnfinishedCount > 0 {
		res.TaskType = TaskTypeReduce

		// Read what Reduce tasks are not being assigned
		for i, reduceTask := range c.ReduceTaskTracker {
			if reduceTask.TaskStatus == TaskStatusPending {
				reduceTask.TaskStatus = TaskStatusRunning
				reduceTask.TaskStartTime = time.Now()
				res.TaskId = i
				return nil
			}
		}

		// All tasks are running by other workers
		res.TaskType = TaskTypeWait
		return nil

	}

	return nil
}

func (c *Coordinator) Report(req *TaskReport, res *TaskReportAck) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.JobStatus {
	case JobStatusMap:
		if req.TaskStatus == TaskStatusFailed {
			log.Printf("Map task %d failed, re-queuing", req.TaskId)
			c.MapTaskTracker[req.TaskId].TaskStatus = TaskStatusPending
			return nil
		}
		if req.TaskStatus == TaskStatusFinished {
			c.MapTaskTracker[req.TaskId].TaskStatus = req.TaskStatus
			c.MapTaskUnfinishedCount--
		}

		// Check all tasks are finished
		if c.MapTaskUnfinishedCount == 0 {
			c.JobStatus = JobStatusReduce
			println("All Map task are finished")
		}

	case JobStatusReduce:
		if req.TaskStatus == TaskStatusFailed {
			log.Printf("Reduce task %d failed, re-queuing", req.TaskId)
			c.ReduceTaskTracker[req.TaskId].TaskStatus = TaskStatusPending
			return nil
		}

		c.ReduceTaskTracker[req.TaskId].TaskStatus = req.TaskStatus
		c.ReduceTaskUnfinishedCount--
		fmt.Printf("c.ReduceTaskTracker %v\n", c.ReduceTaskTracker)

		// Check all tasks are finished
		if c.ReduceTaskUnfinishedCount == 0 {
			c.JobStatus = JobStatusFinished
			println("All Reduce task are finished")

		}
	}
	return nil
}

func (c *Coordinator) HandleOvertime() {
	for {
		time.Sleep(HeartbeatInterval)
		c.mu.Lock()
		if c.JobStatus != JobStatusMap && c.JobStatus != JobStatusReduce {
			c.mu.Unlock()
			return
		}
		switch c.JobStatus {
		case JobStatusMap:
			for _, mapTask := range c.MapTaskTracker {
				if mapTask.TaskStatus == TaskStatusRunning && time.Since(mapTask.TaskStartTime) > TaskTimeout {
					mapTask.TaskStatus = TaskStatusPending
				}
			}
		case JobStatusReduce:
			for _, reduceTask := range c.ReduceTaskTracker {
				if reduceTask.TaskStatus == TaskStatusRunning && time.Since(reduceTask.TaskStartTime) > TaskTimeout {
					reduceTask.TaskStatus = TaskStatusPending
				}
			}
		}
		c.mu.Unlock()
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
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
	go c.HandleOvertime()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.JobStatus == JobStatusFinished {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.MapperNumber = len(files)
	c.ReducerNumber = nReduce
	c.JobStatus = JobStatusMap

	// Set up Map task
	c.MapTaskTracker = make([]*MapTask, c.MapperNumber)
	for i, file := range files {
		c.MapTaskTracker[i] = &MapTask{
			WorkerId:      i,
			TaskStatus:    TaskStatusPending,
			InputFilePath: file}
	}
	c.MapTaskUnfinishedCount = c.MapperNumber
	fmt.Println(c.MapTaskTracker)

	// Set up Reduce task
	c.ReduceTaskTracker = make([]*ReduceTask, c.ReducerNumber)
	for i := range c.ReducerNumber {
		c.ReduceTaskTracker[i] = &ReduceTask{
			WorkerId:   i,
			TaskStatus: TaskStatusPending,
		}
	}
	c.ReduceTaskUnfinishedCount = c.ReducerNumber
	fmt.Println(c.ReduceTaskTracker)

	c.server(sockname)
	return &c
}
