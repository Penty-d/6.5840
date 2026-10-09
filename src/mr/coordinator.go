package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mutex            sync.Mutex
	files            []string
	mapTaskStatus    map[int]string
	reduceTaskStatus map[int]string
	nMap             int
	nReduce          int
}

// 碎碎念：这里稍微思考了下要不要加锁，感觉存在某种情况能用一个递增的数字来分配？（通过原子操作）
func (c *Coordinator) GetTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for taskid, filename := range c.files {
		if c.mapTaskStatus[taskid] == "unassigned" {
			reply.TaskId = taskid
			reply.FileName = filename
			reply.TaskType = "map"
			reply.nReduce = c.nReduce
			c.mapTaskStatus[taskid] = "assigned"
			return nil
		}
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
	}

	// Your code here.

	c.server(sockname)
	return &c
}
