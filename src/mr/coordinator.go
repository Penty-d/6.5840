package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mutex            sync.Mutex
	files            []string
	mapTaskStatus    map[int]string
	reduceTaskStatus map[int]string
	mapTaskStart     map[int]time.Time
	reduceTaskStart  map[int]time.Time
	mapAttempts      map[int]int
	reduceAttempts   map[int]int
	mapCompleted     int
	reduceCompleted  int
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
			expired := c.mapTaskStart[taskid].Add(10*time.Second).Before(time.Now()) && c.mapTaskStatus[taskid] == "assigned"
			if c.mapTaskStatus[taskid] == "" || expired { // 任务未分配或者超时
				c.mapAttempts[taskid]++
				reply.Attempts = c.mapAttempts[taskid]
				reply.TaskId = taskid
				reply.FileName = filename
				reply.TaskType = "map"
				reply.NReduce = c.nReduce
				c.mapTaskStatus[taskid] = "assigned"
				c.mapTaskStart[taskid] = time.Now()
				return nil
			}
		}
		reply.TaskType = "wait"
		return nil
	}
	if c.phase == "reduce" {
		for taskid := 0; taskid < c.nReduce; taskid++ {
			expired := c.reduceTaskStart[taskid].Add(10*time.Second).Before(time.Now()) && c.reduceTaskStatus[taskid] == "assigned"
			if c.reduceTaskStatus[taskid] == "" || expired { // 任务未分配或者超时
				c.reduceAttempts[taskid]++
				reply.Attempts = c.reduceAttempts[taskid]
				reply.TaskId = taskid
				reply.NMap = c.nMap
				reply.TaskType = "reduce"
				c.reduceTaskStatus[taskid] = "assigned"
				c.reduceTaskStart[taskid] = time.Now()
				return nil
			}
		}
		reply.TaskType = "wait"
		return nil
	}
	if c.phase == "done" {
		reply.TaskType = "done"
		return nil
	}
	return nil
}

func (c *Coordinator) ReportMapTask(args *ReportTaskArgs, reply *ReqTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	status, ok := c.mapTaskStatus[args.TaskId]
	if !ok || status != "assigned" || c.mapAttempts[args.TaskId] != args.Attempts { // 任务不存在或者不是assigned状态或者尝试次数不匹配
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
		c.mapCompleted++
		if c.mapCompleted == c.nMap {
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
	if !ok || status != "assigned" || c.reduceAttempts[args.TaskId] != args.Attempts { // 任务不存在或者不是assigned状态或者尝试次数不匹配
		return nil
	}
	if args.TaskStatus == "completed" {
		c.reduceTaskStatus[args.TaskId] = "completed"
		c.reduceCompleted++
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.phase == "reduce" && c.reduceCompleted == c.nReduce {
		c.phase = "done"
		return true
	}
	return c.phase == "done"
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
		mapTaskStart:     make(map[int]time.Time),
		reduceTaskStart:  make(map[int]time.Time),
		mapAttempts:      make(map[int]int),
		reduceAttempts:   make(map[int]int),
		nMap:             len(files),
		phase:            "map",
	}

	c.server(sockname)
	return &c
}
