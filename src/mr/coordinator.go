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
	MapCompleted     int
	ReduceCompleted  int
	nMap             int
	nReduce          int
	phase            string
}

// 碎碎念：这里稍微思考了下要不要加锁，感觉存在某种情况能用一个递增的数字来分配？（通过原子操作）
func (c *Coordinator) GetTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.phase == "map" {
		for taskid, filename := range c.files {
			if c.mapTaskStatus[taskid] == "" {
				reply.TaskId = taskid
				reply.FileName = filename
				reply.TaskType = "map"
				reply.nReduce = c.nReduce
				c.mapTaskStatus[taskid] = "assigned"
				return nil
			}
		}
	}
	if c.phase == "reduce" {
		for taskid := 0; taskid < c.nReduce; taskid++ {
			if c.reduceTaskStatus[taskid] == "" {
				reply.TaskId = taskid
				reply.nMap = c.nMap
				reply.TaskType = "reduce"
				c.reduceTaskStatus[taskid] = "assigned"
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) ReportMapTask(args *ReportTaskArgs, reply *ReqTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	status, ok := c.mapTaskStatus[args.TaskId]
	if !ok || status != "assigned" {
		return nil
	}
	if args.TaskStatus == "completed" {
		c.mapTaskStatus[args.TaskId] = "completed"
		/* 这里的逻辑感觉有点蠢，暂时放这里做反面案例，因为遍历需要o(n)的时间复杂度
		for _, status := range c.mapTaskStatus {
			if status != "completed" {
				return nil
			}
		}
		c.phase = "reduce"
		*/
		c.MapCompleted++
		if c.MapCompleted == c.nMap {
			c.phase = "reduce"
		}
	} else {
		c.mapTaskStatus[args.TaskId] = ""
	}
	return nil
}

func (c *Coordinator) ReportReduceTask(args *ReportTaskArgs, reply *ReqTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	status, ok := c.reduceTaskStatus[args.TaskId]
	if !ok || status != "assigned" {
		return nil
	}
	if args.TaskStatus == "completed" {
		c.reduceTaskStatus[args.TaskId] = "completed"
		c.ReduceCompleted++
	} else {
		c.reduceTaskStatus[args.TaskId] = ""
	}
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	if c.phase == "reduce" && c.ReduceCompleted == c.nReduce {
		return true
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:            files,
		nReduce:          nReduce,
		mapTaskStatus:    make(map[int]string),
		reduceTaskStatus: make(map[int]string),
		nMap:             len(files),
		phase:            "map",
	}

	c.server(sockname)
	return &c
}
