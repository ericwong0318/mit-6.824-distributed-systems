package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	WorkerWaitInterval = 1 * time.Second
	MaxDialRetries     = 5
	DialRetryDelay     = 2 * time.Second
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

type WorkerState struct {
	TaskId            int
	TaskType          TaskType
	TaskInputFileName string
	TaskStatus        TaskStatus

	// For immediate files % calculation
	MapperNumber  int
	ReducerNumber int
}

// Define custom slice type. Need Len(), Swap(), Less() to use sort.Sort()
type ByKey []KeyValue

// Implement Len() int
func (a ByKey) Len() int {
	return len(a)
}

// Implement Swap(i, j int)
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Implement Less(i, j int) bool
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		var req TaskRequest
		var res TaskResponse

		if !call("Coordinator.GetTask", &req, &res) {
			fmt.Printf("call coordinator get task failed")
			break
		}

		var workerState WorkerState
		workerState.TaskId = res.TaskId
		workerState.MapperNumber = res.MapperNumber
		workerState.ReducerNumber = res.ReducerNumber
		workerState.TaskInputFileName = res.TaskInputFileName
		workerState.TaskType = res.TaskType

		switch workerState.TaskType {

		// Map
		case TaskTypeMap:
			if err := doMapTask(&workerState, mapf); err != nil {
				log.Printf("Map task %d failed: %v", workerState.TaskId, err)
				workerState.TaskStatus = TaskStatusFailed
				reportToCoordinator(&workerState)
				continue
			}
			workerState.TaskStatus = TaskStatusFinished
			reportToCoordinator(&workerState)

		// Reduce
		case TaskTypeReduce:
			if err := doReduceTask(&workerState, reducef); err != nil {
				log.Printf("Reduce task %d failed: %v", workerState.TaskId, err)
				workerState.TaskStatus = TaskStatusFailed
				reportToCoordinator(&workerState)
				continue
			}
			workerState.TaskStatus = TaskStatusFinished
			reportToCoordinator(&workerState)

		// Wait
		case TaskTypeWait:
			println("Wait for 1 second")
			time.Sleep(WorkerWaitInterval)

		// Finish
		case TaskTypeFinish:
			println("Worker is exited")
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(workerState *WorkerState, mapf func(string, string) []KeyValue) error {
	println("Map task started")
	content, err := os.ReadFile(workerState.TaskInputFileName)
	if err != nil {
		return fmt.Errorf("cannot read %v: %w", workerState.TaskInputFileName, err)
	}
	kv_arr := mapf(workerState.TaskInputFileName, string(content))

	// Write to buckets
	buckets := make([][]KeyValue, workerState.ReducerNumber)
	for _, kv := range kv_arr {
		// Use ihash to distribute key across reducers
		reducerFileIdx := ihash(kv.Key) % workerState.ReducerNumber // Write to buckets
		buckets[reducerFileIdx] = append(buckets[reducerFileIdx], kv)
	}

	// Write to intermediate files
	for reducerFileIdx := range workerState.ReducerNumber {
		tmpFileName := fmt.Sprintf("mr-tmp-%d-%d", workerState.TaskId, reducerFileIdx)
		tmpFile, err := os.CreateTemp(".", tmpFileName)
		if err != nil {
			return fmt.Errorf("create temp file failed: %w", err)
		}
		encoder := json.NewEncoder(tmpFile)
		err = encoder.Encode(&buckets[reducerFileIdx])
		if err != nil {
			tmpFile.Close()
			return fmt.Errorf("encoder encode failed: %w", err)
		}
		tmpFile.Close()

		// println(tmpFileName)

		intermediateFileName := fmt.Sprintf("mr-%d-%d", workerState.TaskId, reducerFileIdx)
		err = os.Rename(tmpFile.Name(), intermediateFileName)
		if err != nil {
			os.Remove(tmpFileName)
			return fmt.Errorf("rename temp file failed: %w", err)
		}
	}

	println("Map Task Completed")
	return nil
}

func doReduceTask(workerState *WorkerState, reducef func(string, []string) string) error {
	var kva []KeyValue
	for i := range workerState.MapperNumber {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, workerState.TaskId)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			return fmt.Errorf("open file failed: %w", err)
		}
		fmt.Printf("intermediateFile %v\n", intermediateFile.Name())
		decoder := json.NewDecoder(intermediateFile)
		for {
			var kv []KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				intermediateFile.Close()
				return fmt.Errorf("decoder decode failed: %w", err)
			}

			kva = append(kva, kv...)
		}
		intermediateFile.Close()
	}

	println("Sort started")
	sort.Sort(ByKey(kva))
	// fmt.Printf("kva: %v", kva)

	println("Reduce task started")

	// Use 2 pointers to find out groups with same key

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		// fmt.Printf("k: %v v: %v\n", kva[i].Key, values)
		ret := reducef(kva[i].Key, values)

		// Output to files
		outputFileName := fmt.Sprintf("mr-out-%d", workerState.TaskId)
		outputFile, err := os.OpenFile(outputFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return fmt.Errorf("output file failed: %w", err)
		}
		// fmt.Printf("ret: %v\n", ret)
		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, ret)
		i = j

		outputFile.Close()
	}

	println("Reduce task completed")
	return nil
}

func reportToCoordinator(workerState *WorkerState) {
	var req TaskReport
	req.TaskId = workerState.TaskId
	req.TaskStatus = workerState.TaskStatus
	fmt.Printf("report %v\n", req)
	var res TaskReportAck
	if !call("Coordinator.Report", &req, &res) {
		fmt.Printf("call coordinator get report failed")
	}
	println("Report to Coordinator Completed")
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	var c *rpc.Client
	var err error

	// Retry dialing with backoff
	for attempt := 0; attempt < MaxDialRetries; attempt++ {
		// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
		c, err = rpc.DialHTTP("unix", coordSockName)
		if err == nil {
			break
		}
		log.Printf("dial attempt %d/%d failed: %v", attempt+1, MaxDialRetries, err)
		time.Sleep(DialRetryDelay)
	}
	if err != nil {
		log.Printf("%d: dialing failed after %d attempts: %v", os.Getpid(), MaxDialRetries, err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
