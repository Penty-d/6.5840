package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type ReqTaskArgs struct {
}

type ReqTaskReply struct {
	Attempts int
	TaskId   int
	NReduce  int
	NMap     int
	FileName string
	TaskType string
}

type ReportTaskArgs struct {
	Attempts   int
	TaskId     int
	TaskStatus string
}
