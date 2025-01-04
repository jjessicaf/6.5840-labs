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
	kv            map[string]string
	requestCache  map[int64]string
	appendIdCache map[string]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	reply.Value = kv.kv[args.Key] //  By default Go returns empty zero-value if key doesn't exist
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.requestCache[args.Id]; exists {
		return
	}

	kv.kv[args.Key] = args.Value
	kv.requestCache[args.Id] = ""
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if old, exists := kv.requestCache[args.Id]; exists {
		reply.Value = old
		return
	}

	old := kv.kv[args.Key]
	if old == "" {
		kv.kv[args.Key] = args.Value
	} else {
		kv.kv[args.Key] += args.Value
	}

	kv.requestCache[args.Id] = old
	if id, exists := kv.appendIdCache[args.Key]; exists {
		delete(kv.requestCache, id)
		kv.appendIdCache[args.Key] = args.Id
	}
	reply.Value = old
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.requestCache = make(map[int64]string)
	kv.appendIdCache = make(map[string]int64)

	return kv
}
