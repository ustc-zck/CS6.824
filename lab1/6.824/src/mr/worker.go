package mr

import (
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		fileName, nReduce, err := callDispatchTask()
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		start := time.Now()

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			continue
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
			continue
		}

		fileMap := map[string]*os.File{}
		for i := 0; i < nReduce; i++ {
			tmpFile := fmt.Sprintf("immediate-%d.txt", i)
			_, err := os.Stat(tmpFile)
			if err != nil {
				_, err := os.Create(tmpFile)
				if err != nil {
					fmt.Printf("failed to create file")
					return
				}
			}
			f, err := os.OpenFile(tmpFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Printf("failed to open file")
				return
			}
			fileMap[tmpFile] = f
		}

		kva := mapf(fileName, string(content))

		for _, kv := range kva {
			//create immediate file
			tmpFile := fmt.Sprintf("immediate-%d.txt", computeCrc32(kv.Key)%uint32(nReduce))
			f := fileMap[tmpFile]
			if _, err := f.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value)); err != nil {
				fmt.Printf("failed to write string into file: %s", err.Error())
				continue
			}
		}
		if err := CallSubTaskDone(fileName); err != nil {
			fmt.Printf("failed to set sub task done:%s", err.Error())
			return
		}
		fmt.Printf("mapping file name %s cost %f\n", fileName, time.Now().Sub(start).Seconds())

		for i := 0; i < nReduce; i++ {
			tmpFile := fmt.Sprintf("immediate-%d.txt", i)
			f := fileMap[tmpFile]
			f.Close()
		}
	}

	//reduce...
	for {
		fileName, err := callDispatchReduceTask()
		if err != nil {
			fmt.Printf("failed to get reduce task file name")
			break
		}
		fmt.Printf("get reduce task file name:%s", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("failed to open file:%s", err.Error())
			continue
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Printf("failed to read content:%s", err.Error())
			continue
		}

		intermediate := []KeyValue{}
		for _, line := range strings.Split(string(content), "\n") {
			items := strings.Split(line, " ")
			if len(items) == 2 {
				kv := KeyValue{Key: items[0], Value: items[1]}
				intermediate = append(intermediate, kv)
			}
		}
		sort.Sort(ByKey(intermediate))

		oname := "mr-out-0"
		ofile, _ := os.Create(oname)

		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
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

func CallSubTaskDone(fileName string) error {
	args := SubTaskDoneArgs{FileName: fileName}
	reply := SubTaskDoneReply{}

	ok := call("Coordinator.SubTaskDone", &args, &reply)
	if ok {
		fmt.Printf("call sub task done!\n")
		return nil
	} else {
		fmt.Printf("call sub task failed!\n")
		return errors.New("call sub task failed!")
	}
}

func callDispatchTask() (string, int, error) {
	args := DispatchArgs{}
	reply := DispatchReply{}
	ok := call("Coordinator.DispatchTask", &args, &reply)
	if ok {
		fmt.Printf("call dispath task done!\n")
	} else {
		fmt.Printf("call dispatch task failed!\n")
		return "", -1, errors.New(fmt.Sprintf("call dispatch task failed!\n"))
	}
	if reply.FileName == "" || reply.NReduce == 0 {
		return "", -1, errors.New("not dispathed a task!")
	}
	return reply.FileName, reply.NReduce, nil
}

func callDispatchReduceTask() (string, error) {
	args := DispatchReduceTaskArgs{}
	reply := DispatchReduceTaskReply{}
	ok := call("Coordinator.DispatchReduceTask", &args, &reply)
	if ok {
		fmt.Printf("call dispath reduce task done!\n")
	} else {
		fmt.Printf("call dispatch reduce  task failed!\n")
		return "", errors.New(fmt.Sprintf("call dispatch task failed!\n"))
	}
	if reply.FileName == "" {
		return "", errors.New("not dispathed a task!")
	}
	return reply.FileName, nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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

func computeCrc32(key string) uint32 {
	crc32q := crc32.MakeTable(0xD5828281)
	return crc32.Checksum([]byte(key), crc32q)
}
