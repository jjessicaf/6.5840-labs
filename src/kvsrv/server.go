package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	reply.Value = kv.kv[args.Key] //  By default Go returns empty zero-value if key doesn't exist
	//log.Printf("Log: server get value: %s", reply.Value)
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.kv[args.Key] = args.Value
	//log.Printf("Log: server put value: %s", kv.kv[args.Key])
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	old := kv.kv[args.Key]
	if old == "" {
		kv.kv[args.Key] = args.Value
	} else {
		kv.kv[args.Key] += args.Value
	}
	kv.mu.Unlock()
	reply.Value = old
	//log.Printf("Log: server append old value: %s", old)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)

	return kv
}
