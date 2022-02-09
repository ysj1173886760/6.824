package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

const (
	Wait int = 0
	Done 	 = 1
	EXEC     = 2
	TimeOut  = 3
)

type MapTask struct {
	filename string
	status int
}

type ReduceTask struct {
	status int
}

type Master struct {
	// Your definitions here.
	mutex sync.Mutex

	R int
	M int

	map_tasks []MapTask
	reduce_tasks []ReduceTask

	map_task_timer []time.Time
	reduce_task_timer []time.Time

	finished_map int
	finished_reduce int
	all_done bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// iterate all tasks and find an valid one
	for i := 0; i < m.M; i++ {
		if m.map_tasks[i].status == Wait || m.map_tasks[i].status == TimeOut {
			// find one
			reply.TaskType = Map
			reply.ID = i
			reply.FileName = m.map_tasks[i].filename
			reply.M = m.M
			reply.R = m.R
			m.map_tasks[i].status = EXEC
			// set the timer
			m.map_task_timer[i] = time.Now()
			// log.Printf("Sending Map Task %v", reply)
			return nil
		}
	}

	if m.finished_map != m.M {
		// try later
		reply.TaskType = None
		return nil
	}

	for i := 0; i < m.R; i++ {
		if m.reduce_tasks[i].status == Wait || m.reduce_tasks[i].status == TimeOut {
			reply.TaskType = Reduce
			reply.ID = i
			reply.M = m.M
			reply.R = m.R
			reply.FileName = ""
			m.reduce_tasks[i].status = EXEC
			m.reduce_task_timer[i] = time.Now()
			// log.Printf("Sending Reduce Task %v", reply)
			return nil
		}
	}

	// otherwise, we return None
	reply.TaskType = None
	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch args.TaskType {
	case Map:
		if m.map_tasks[args.ID].status != Done {
			m.finished_map++
			m.map_tasks[args.ID].status = Done
		}
	case Reduce:
		if m.reduce_tasks[args.ID].status != Done {
			m.finished_reduce++
			m.reduce_tasks[args.ID].status = Done
		}
	}

	if m.finished_map == m.M && m.finished_reduce == m.R {
		m.all_done = true
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.


	return m.all_done
}

// background thread aimed to find timeout tasks
func (m *Master) timer() {
	for !m.all_done {
		m.mutex.Lock()
		for i := 0; i < m.M; i++ {
			if m.map_tasks[i].status == EXEC && 
			   time.Now().Sub(m.map_task_timer[i]) > time.Second * 10 {
				m.map_tasks[i].status = TimeOut
			}
		}
		for i := 0; i < m.R; i++ {
			if m.reduce_tasks[i].status == EXEC && 
			   time.Now().Sub(m.reduce_task_timer[i]) > time.Second * 10 {
				m.reduce_tasks[i].status = TimeOut
			}
		}
        m.mutex.Unlock()
        time.Sleep(time.Second * 5)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.R = nReduce
	m.M = len(files)

	m.map_tasks = make([]MapTask, m.M)
	for idx, file := range files {
		m.map_tasks[idx].filename = file
		m.map_tasks[idx].status = Wait
	}

	m.reduce_tasks = make([]ReduceTask, m.R)
	for i := 0; i < m.R; i++ {
		m.reduce_tasks[i].status = Wait
	}

    m.map_task_timer = make([]time.Time, m.M)
    m.reduce_task_timer = make([]time.Time, m.R)

	m.finished_map = 0
	m.finished_reduce = 0
	m.all_done = false

    go m.timer()
	m.server()
	return &m
}
