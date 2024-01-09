package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
)

// Used to store intermediate MR output
const TempDir = "temp"

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

func requestTask() (reply *RequestTaskReply, succ bool) {
	args := RequestTaskArgs{WorkerID: os.Getpid()}
	reply = &RequestTaskReply{}
	succ = call("Coordinator.RequestTask", &args, reply)
	return reply, succ
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	args := GetNReduceArgs{}
	reply := GetNReduceReply{}
	call("Coordinator.GetNReduce", &args, &reply)
	nReduce := reply.NReduce

	for {
		// 1. Request a task from the coordinator
		reply, succ := requestTask()

		if !succ {
			// fmt.Println("Request task failed")
			return
		}

		if reply.TaskType == ExitTask {
			// fmt.Println("No task left")
			return
		}

		if reply.TaskType == MapTask {
			// 2. Do the map task
			// fmt.Printf("Map task %v, filename: %s, nReduce: %v\n", reply.TaskId, reply.File, nReduce)
			doMap(mapf, reply.File, reply.TaskId, nReduce)
			// 3. Report the task is done to the coordinator
			reportMapDone(reply.TaskId)
		} else {
			// 2. Do the reduce task
			doReduce(reducef, reply.TaskId)
			// 3. Report the task is done to the coordinator
			reportReduceDone(reply.TaskId)
		}

		// 4. Report the task is done to the coordinator
		// 5. Go back to step 1

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func reportReduceDone(TaskId int) {
	args := ReportTaskDoneArgs{TaskType: ReduceTask, TaskId: TaskId}
	reply := ReportTaskDoneReply{}
	call("Coordinator.ReportTaskDone", &args, &reply)
}

func doReduce(reducef func(string, []string) string, reduceID int) {
	// println("do reduce called once")
	// 1. Read all the intermediate files
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-*-%d", TempDir, reduceID))
	if err != nil {
		log.Fatalf("cannot read temp files")
	}
	// 2. Group the files by key

	intermediate := make(map[string][]string)

	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("cannot open temp file")
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}

		file.Close()
	}

	// 3. Call reducef on each group
	outputFile, err := ioutil.TempFile(TempDir, "mr-out-")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	defer outputFile.Close()

	for key, values := range intermediate {
		output := reducef(key, values)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}

	// 4. Write the output to a file
	finalFilename := fmt.Sprintf("mr-out-%d", reduceID)
	os.Rename(outputFile.Name(), finalFilename)
}

func reportMapDone(TaskId int) {
	args := ReportTaskDoneArgs{TaskType: MapTask, TaskId: TaskId}
	reply := ReportTaskDoneReply{}
	call("Coordinator.ReportTaskDone", &args, &reply)
}

func doMap(mapf func(string, string) []KeyValue, filename string, mapID int, nReduce int) {
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

	writeMapOutput(kva, mapID, nReduce)
}

func writeMapOutput(kva []KeyValue, mapID int, nReduce int) {
	tempFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile(TempDir, "intermediate-")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		tempFiles[i] = tempFile
		encoders[i] = json.NewEncoder(tempFiles[i])
	}

	for _, kv := range kva {
		// fmt.Println(nReduce)
		reduceID := ihash(kv.Key) % nReduce
		err := encoders[reduceID].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv")
		}
		if err != nil {
			log.Fatalf("cannot encode kv")
		}
	}

	for i, tempFile := range tempFiles {
		tempFile.Close()
		filename := fmt.Sprintf("%v/mr-%d-%d", TempDir, mapID, i)
		os.Rename(tempFile.Name(), filename)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
	return false
}
