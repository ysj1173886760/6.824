package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// as far as we are receiving None, indicating that there is no more tasks waiting to be executed
// but we still need to request tasks because there may have a crash
const (
	Map int = 0
	Reduce  = 1
	None    = 2
)

type RequestTaskArgs struct {
	// None
}

type RequestTaskReply struct {
	TaskType int
	// task id
	ID int
	// for reduce worker to read intermediate file
	M int
	// for map worker to generate intermediate file
	R int
	// filename for map task
	FileName string
}

type FinishTaskArgs struct {
	TaskType int
	ID int
}

type FinishTaskReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
