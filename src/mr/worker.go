package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "github.com/google/uuid"
import "time"
import "io/ioutil"
import "encoding/json"
import "os"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func persistMapRes(mapTask Task, kvs []KeyValue, nReduce int) []string {
	// partition kvs into nReduce partitions
	bucketNumToKvs := make(map[int][]KeyValue)
	for _, kv := range kvs {
		bucketNum := ihash(kv.Key) % nReduce
		bucketNumToKvs[bucketNum] = append(bucketNumToKvs[bucketNum], kv)
	}
	intermediateFnames := []string{}
	for bucketNum, Kvs := range bucketNumToKvs {
		destFname := fmt.Sprintf("mr_%v_%v", mapTask.Id, bucketNum)
		contentBytes, err := json.Marshal(Kvs)
		err = ioutil.WriteFile(destFname, contentBytes, 0644)
		checkError(err)
		intermediateFnames = append(intermediateFnames, destFname)
	}
	return intermediateFnames
}

func execMap(workerId string, mapf func(string, string) []KeyValue) {
	for {
		task, nReduce := CallAssignMap(workerId)
		if task.TaskType == AllCompleted {
			break
		} else if task.TaskType == MapTask {
			inputFname := task.Fnames[0]
			fileBytes, err := ioutil.ReadFile(inputFname)
			checkError(err)
			content := string(fileBytes)
			mapfRes := mapf(inputFname, content)
			intermediateFnames := persistMapRes(task, mapfRes, nReduce)
			fmt.Printf("intermediateFnames: %v\n", intermediateFnames)
			checkError(CallReportMapComplete(workerId, task.Id, intermediateFnames))
		}
		time.Sleep(2 * time.Second)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := uuid.NewString()
	// TODO
	execMap(workerId, mapf)
	for {
		task := CallAssignReduce(workerId)
		if task.TaskType == AllCompleted {
			break
		} else if task.TaskType == ReduceTask {
			fnames := task.Fnames
			kMap := map[string][]string{}
			for _, fname := range fnames {
				jsonBlob, err := ioutil.ReadFile(fname)
				checkError(err)
				kvs := &([]KeyValue{})
				json.Unmarshal(jsonBlob, kvs)
				for _, kv := range *kvs {
					kMap[kv.Key] = append(kMap[kv.Key], kv.Value)
				}
			}
			f, err := os.OpenFile(fmt.Sprintf("mr_out_%v", task.Id), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			for k, vs := range kMap {
				reduceRes := fmt.Sprintf("key: %v, value: %v\n", k, reducef(k, vs))
				if _, err = f.WriteString(reduceRes); err != nil {
					panic(err)
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func CallReportMapComplete(wid string, taskId string, intermediateFnames []string) error {
	args := ReportMapCompleteArgs{wid, taskId, intermediateFnames}
	reply := ReportMapCompleteReply{}
	call("Coordinator.ReportMapComplete", &args, &reply)
	return reply.Err
}

func CallAssignMap(workerId string) (Task, int) {
	args := AssignMapArgs{workerId}
	reply := AssignMapReply{}
	call("Coordinator.AssignMap", &args, &reply)
	fmt.Printf("CallAssignMap: %v\n", reply.Task)
	return reply.Task, reply.NReduce
}

func CallAssignReduce(workerId string) Task {
	args := AssignReduceArgs{workerId}
	reply := AssignReduceReply{}
	call("Coordinator.AssignReduce", &args, &reply)
	fmt.Printf("CallAssignReduce: %v\n", reply.Task)
	return reply.Task
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	panic(err)
}
