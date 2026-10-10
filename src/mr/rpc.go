package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type ReqTaskArgs struct {
}

type ReqTaskReply struct {
	TaskId   int
	nReduce  int
	nMap     int
	FileName string
	TaskType string
}

type ReportTaskArgs struct {
	TaskId     int
	TaskStatus string
}
