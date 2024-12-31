package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Request a task
		response := CallRequestTask()

		if response.TaskType == "done" {
			log.Println("Worker: No more tasks, shutting down.")
			break
		}
		if response.TaskType == "map" {
			filename := response.Filename
			tn := response.TaskNumber

			// Read file and call application Map
			kva := performMap(filename, mapf)

			// Each mapper should create nReduce intermediate files for consumption by the reduce tasks
			storeIntermediate(kva, response.N, tn)

			CallTaskDone(response.TaskType, tn)
		}
		if response.TaskType == "reduce" {
			tn := response.TaskNumber

			intermediate := getIntermediate(response.N, tn)

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", tn)
			performReduce(oname, intermediate, reducef)

			CallTaskDone(response.TaskType, tn)
		}

		time.Sleep(time.Second) // Workers to periodically ask the coordinator for work, sleeping with time.Sleep() between each request
	}
}

func CallRequestTask() RequestTaskResponse {
	// declare an argument structure.
	args := RequestTaskArgs{}

	// declare a reply structure.
	reply := RequestTaskResponse{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		// If the call fails, it likely means the coordinator has finished, so we return a "done" task.
		reply.TaskType = "done"
	}

	return reply
}

func CallTaskDone(taskType string, taskNumber int) TaskDoneResponse {
	// declare an argument structure.
	args := TaskDoneArgs{}
	args.TaskType = taskType
	args.TaskNumber = taskNumber

	// declare a reply structure.
	reply := TaskDoneResponse{}

	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		log.Printf("Failed to report task completion: TaskType=%v, TaskNumber=%v", taskType, taskNumber)
	}

	return reply
}

func performMap(filename string, mapf func(string, string) []KeyValue) []KeyValue {
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

	return kva
}

// nReduce intermediate files to store intermediates, use hash() % nReduce to determine which bucket to assign to
// Each mapper should create nReduce intermediate files for consumption by the reduce tasks
func storeIntermediate(kva []KeyValue, nReduce int, X int) error {
	files := make(map[int]*os.File)
	encoders := make(map[int]*json.Encoder)
	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		if _, exists := files[partition]; !exists {
			filename := fmt.Sprintf("mr-%d-%d.txt", X, partition)
			file, err := os.Create(filename)
			if err != nil {
				return fmt.Errorf("cannot create file %v: %w", filename, err)
			}
			files[partition] = file
			encoders[partition] = json.NewEncoder(file)
		}
		if err := encoders[partition].Encode(&kv); err != nil {
			return fmt.Errorf("cannot encode kv pair: %w", err)
		}
	}

	for _, file := range files {
		file.Close()
	}

	return nil
}

func getIntermediate(numMapTasks int, partition int) []KeyValue {
	var intermediate []KeyValue
	for i := 0; i < numMapTasks; i++ {
		filename := fmt.Sprintf("mr-%d-%d.txt", i, partition)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	return intermediate
}

func performReduce(oname string, intermediate []KeyValue, reducef func(string, []string) string) {
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create file %v: %v", oname, err)
	}

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
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
