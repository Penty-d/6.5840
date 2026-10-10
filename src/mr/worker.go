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

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	arg := ReqTaskArgs{}
	reply := ReqTaskReply{}
	if !call("Coordinator.GetTask", &arg, &reply) {
		log.Fatalf("call failed!") // call不到就不再call了
	}
	if reply.TaskType == "map" {
		content, err := os.ReadFile(reply.FileName)
		if err != nil {
			call("Coordinator.ReportMapTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
			log.Fatalf("cannot read %v", reply.FileName)
		}
		kvs := mapf(reply.FileName, string(content))
		buckets := make([][]KeyValue, reply.nReduce)
		for _, kv := range kvs {
			bucket := ihash(kv.Key) % reply.nReduce
			buckets[bucket] = append(buckets[bucket], kv)
		}
		for i, bucket := range buckets {
			if len(bucket) == 0 {
				continue
			}
			// 这里使用临时文件是因为coordinator需要超时重连，对于超时的worker，reduce可能读到错误信息
			tmpFile, err := os.CreateTemp("", "mr-tmp-*")
			if err != nil {
				call("Coordinator.ReportMapTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
				log.Fatalf("cannot create temp file: %v", err)
			}
			enc := json.NewEncoder(tmpFile)
			for _, kv := range bucket {
				if err := enc.Encode(&kv); err != nil {
					call("Coordinator.ReportMapTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
					log.Fatalf("cannot encode kv: %v", err)
				}
			}
			outFileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
			tmpFile.Close()
			if err := os.Rename(tmpFile.Name(), outFileName); err != nil {
				call("Coordinator.ReportMapTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
				log.Fatalf("cannot rename temp file: %v", err)
			}
		}
		call("Coordinator.ReportMapTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "completed"}, &ReqTaskReply{})
	} else if reply.TaskType == "reduce" {
		intermediate := []KeyValue{}
		for i := 0; i < reply.nMap; i++ {
			fileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
			file, err := os.Open(fileName)
			if err != nil {
				call("Coordinator.ReportReduceTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
				log.Fatalf("cannot open %v", fileName)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				err := dec.Decode(&kv)
				if err == io.EOF {
					break
				}
				if err != nil {
					call("Coordinator.ReportReduceTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
					log.Fatalf("cannot decode kv: %v", err)
				}
				intermediate = append(intermediate, kv)
			}
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})

			tmpFile, err := os.CreateTemp("", "mr-rd-tmp-*")
			if err != nil {
				call("Coordinator.ReportReduceTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
				log.Fatalf("cannot create temp file: %v", err)
			}

			for i := 0; i < len(intermediate); {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				i = j
				values := make([]string, 0, j-i)
				for k := i - (j - i); k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				if _, err := fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output); err != nil {
					call("Coordinator.ReportReduceTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
					log.Fatalf("cannot write to temp file: %v", err)
				}
				i = j
			}
			if err := os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", reply.TaskId)); err != nil {
				call("Coordinator.ReportReduceTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "failed"}, &ReqTaskReply{})
				log.Fatalf("cannot rename temp file: %v", err)
			}
			tmpFile.Close()
			os.Remove(fileName)
			call("Coordinator.ReportReduceTask", &ReportTaskArgs{TaskId: reply.TaskId, TaskStatus: "completed"}, &ReqTaskReply{})
		}
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
