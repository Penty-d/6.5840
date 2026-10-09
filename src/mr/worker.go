package mr

import (
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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
		log.Fatalf("call failed!")
	}
	content, err := os.ReadFile(reply.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	kvs := mapf(reply.FileName, string(content))
	buckets := make([][]KeyValue, reply.nReduce)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % reply.nReduce
		buckets[bucket] = append(buckets[bucket], kv)
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
