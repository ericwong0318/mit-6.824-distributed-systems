package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Task Type
type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
	TaskTypeFinish
)

// Task Status
type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusRunning
	TaskStatusFinished
	TaskStatusFailed
)

// KeyValue is the type emitted by Map functions.
type TaskRequest struct {
}

type TaskResponse struct {
	TaskId            int
	TaskType          TaskType
	TaskInputFileName string

	// For immediate files % calculation
	MapperNumber  int
	ReducerNumber int
}

type TaskReport struct {
	TaskId     int
	TaskStatus TaskStatus
}

type TaskReportAck struct {
}
