package mr

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"regexp"
	"strconv"
)

type TaskType int32

const (
	MapTask       TaskType = 0
	ReduceTask    TaskType = 1
	AllCompleted  TaskType = 2
	AllInProgress TaskType = 3
	NoneAvailable TaskType = 4
)

type TaskStatus int32

const (
	idle       = 0
	inProgress = 1
	completed  = 2
)

type Task struct {
	Id       string
	Fnames   []string
	TaskType TaskType
	Status   TaskStatus
	WorkerId string
}

type Tasks struct {
	taskMap map[string]*Task
	mu      sync.Mutex
}

type Coordinator struct {
	mapTasks              *Tasks
	reduceTasks           *Tasks
	reduceTaskOrder []string
	workerToTasks         map[string][]*Task
	workerToLastHeartBeat map[string]time.Time
	nReduce               int
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func (c *Coordinator) assign(workerId string, tasks *Tasks) *Task {
	(*tasks).mu.Lock()
	defer (*tasks).mu.Unlock()
	c.workerToLastHeartBeat[workerId] = time.Now()

	if len(tasks.taskMap) == 0 {
		t := new(Task)
		t.TaskType = NoneAvailable
		return t
	}
	allInProgress := true
	allCompleted := true
	for _, task := range tasks.taskMap {
		if task.Status == idle {
			task.Status = inProgress
			task.WorkerId = workerId
			c.workerToTasks[workerId] = append(c.workerToTasks[workerId], task)
			return task
		}
		if task.Status != inProgress {
			allInProgress = false
		}
		if task.Status != completed {
			allCompleted = false
		}
	}
	signalTask := new(Task)
	if allInProgress {
		signalTask.TaskType = AllInProgress
	} else if allCompleted {
		signalTask.TaskType = AllCompleted
	}
	return signalTask
}

func (c *Coordinator) AssignMap(args *AssignMapArgs, reply *AssignMapReply) error {
	reply.Task = *(c.assign(args.WorkerId, c.mapTasks))
	reply.NReduce = c.nReduce
	return nil

}

func (c *Coordinator) AssignReduce(args *AssignReduceArgs, reply *AssignReduceReply) error {
	reply.Task = *(c.assign(args.WorkerId, c.reduceTasks))
	return nil
}

func (c *Coordinator) ReportMapComplete(args *ReportMapCompleteArgs, reply *ReportMapCompleteReply) error {
	workerId := args.WorkerId
	taskId := args.TaskId
	intermediateFnames := args.IntermediateFnames
	c.mapTasks.taskMap[taskId].Status = completed
	delete(c.workerToTasks, workerId)

	for _, fname := range intermediateFnames {
		re := regexp.MustCompile(`\d+$`)
		reducePartitionBytes := re.Find([]byte(fname))
		reducePartitionNum, _ := strconv.Atoi(string(reducePartitionBytes))
		taskId := c.reduceTaskOrder[reducePartitionNum]
		reduceTaskToAdd := c.reduceTasks.taskMap[taskId]
		reduceTaskToAdd.Fnames = append(reduceTaskToAdd.Fnames, fname)
		// fmt.Printf("num: %v, taskId: %v, reduceTaskToAdd.Fnames\n", reducePartitionNum, taskId, reduceTaskToAdd.Fnames)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := false
	return ret
}

func secondsFromNow(t time.Time) int64 {
	return int64(time.Now().Sub(t) / time.Second)
}

func removeZombieWorker(workerToTasks map[string][]*Task, workerToLastHeartBeat map[string]time.Time, wid string) {
	delete(workerToLastHeartBeat, wid)
	for _, task := range workerToTasks[wid] {
		if task.TaskType == MapTask || task.TaskType == ReduceTask {
			task.Status = idle
			task.WorkerId = ""
		}
	}
	delete(workerToTasks, wid)
}

func (c *Coordinator) printSysState() {
	fmt.Printf("workerToHeartbeats: %v\n", c.workerToLastHeartBeat)
	fmt.Printf("workerToTasks: %v\n", c.workerToTasks)

	fmt.Printf("mapTasks>>>")
	for _, task := range c.mapTasks.taskMap {
		fmt.Printf("|%v|", *task)
	}
	fmt.Printf("\n")

	fmt.Printf("reduceTasks>>>")
	for _, task := range c.reduceTasks.taskMap {
		fmt.Printf("|%v|", *task)
	}
	fmt.Printf("\n")
}

func (c *Coordinator) monitorWorkers() {
	for {
		c.printSysState()
		threashold := int64(10)
		for wid, lastBeat := range c.workerToLastHeartBeat {
			if secondsFromNow(lastBeat) >= threashold {
				removeZombieWorker(c.workerToTasks, c.workerToLastHeartBeat, wid)
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:              &Tasks{taskMap: make(map[string]*Task)},
		reduceTasks:           &Tasks{taskMap: make(map[string]*Task)},
		workerToTasks:         make(map[string][]*Task),
		workerToLastHeartBeat: make(map[string]time.Time),
	}
	for _, fname := range files {
		taskFnames := []string{fname}
		taskId := uuid.NewString()
		c.mapTasks.taskMap[taskId] = &Task{
			Id:       taskId,
			Fnames:   taskFnames,
			TaskType: MapTask,
			Status:   idle,
			WorkerId: "",
		}
	}
	
	for i := 0; i < nReduce; i++ {
		taskFnames := []string{}
		taskId := uuid.NewString()
		c.reduceTasks.taskMap[taskId] = &Task{
			Id:       taskId,
			Fnames:   taskFnames,
			TaskType: ReduceTask,
			Status:   idle,
			WorkerId: "",
		}
		c.reduceTaskOrder = append(c.reduceTaskOrder, taskId)
	}
	c.nReduce = nReduce
	c.server()
	go c.monitorWorkers()
	return &c
}
