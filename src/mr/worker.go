package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "io/ioutil"
import "encoding/json"
import "os"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		success := call("Master.RequestTask", &args, &reply)

		// if we can't connect to master, which indicate that all tasks has been completed, then we exit
		if !success {
			break;
		}

		switch reply.TaskType {
		case None:
			time.Sleep(time.Second)
		case Map:
			MapWorker(mapf, &reply)
		case Reduce:
			ReduceWorker(reducef, &reply)
		}
	}
}

func MapWorker(mapf func(string, string) []KeyValue, args *RequestTaskReply) {
	filename := args.FileName

	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// now partition
	file_list := make([]*os.File, args.R)
	encoder_list := make([]*json.Encoder, args.R)

	for i := 0; i < args.R; i++ {
		file, err := ioutil.TempFile(".", "map*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		enc := json.NewEncoder(file)

		file_list[i] = file
		encoder_list[i] = enc
	}

	for _, kv := range intermediate {
		p := ihash(kv.Key) % args.R
		err := encoder_list[p].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v %v", kv.Key, kv.Value)
		}
	}

	// then rename the files
	for i := 0; i < args.R; i++ {
		os.Rename(file_list[i].Name(), fmt.Sprintf("mr-%v-%v", args.ID, i))
		file_list[i].Close()
	}

	// tell the master that work has been done
	finish_args := FinishTaskArgs{}
	finish_args.TaskType = Map
	finish_args.ID = args.ID
	finish_reply := FinishTaskReply{}

	call("Master.FinishTask", &finish_args, &finish_reply)
}

func ReduceWorker(reducef func(string, []string) string, args *RequestTaskReply) {
	intermediate := []KeyValue{}
	id := args.ID
	
	// read intermediate result
	for i := 0; i < args.M; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%v-%v", i, id))
		if err != nil {
			log.Fatalf("failed to read %v", fmt.Sprintf("mr-%v-%v", i, id))
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort
	sort.Sort(ByKey(intermediate))

	// do reduce
	oname := fmt.Sprintf("mr-out-%v", id)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	// tell the master that work has been done
	finish_args := FinishTaskArgs{}
	finish_args.TaskType = Reduce
	finish_args.ID = args.ID
	finish_reply := FinishTaskReply{}

	call("Master.FinishTask", &finish_args, &finish_reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
	return false
}
