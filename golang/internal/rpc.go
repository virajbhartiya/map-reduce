package mr

import (
	"os"
	"strconv"
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Name    string
	Number  int
	NReduce int
	Type    TaskType
}

type UpdateTaskStatusArgs struct {
	Name string
	Type TaskType
}

type UpdateTaskStatusReply struct {
}

type TaskType string

var (
	mType TaskType = "map"
	rType TaskType = "reduce"
)

func coordinatorSock() string {
	s := "/var/tmp/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
