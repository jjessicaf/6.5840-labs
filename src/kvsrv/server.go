package kvsrv

import (
	"log"
	"strings"
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
	cond         *sync.Cond
	kv           map[string]string
	pending      map[int64]bool // Tracks pending RPCs by ClientId+Seq
	requestCache map[int64]int  // Client ID to last sequence number
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	reply.Value = kv.kv[args.Key] //  By default Go returns empty zero-value if key doesn't exist
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.ClientId*1000000 + int64(args.Seq) // Unique key for this RPC

	for kv.pending[key] {
		kv.cond.Wait()
	}

	if seq, exists := kv.requestCache[args.ClientId]; exists && args.Seq <= seq {
		return
	}

	kv.pending[key] = true

	kv.kv[args.Key] = args.Value
	kv.requestCache[args.ClientId] = args.Seq
	reply.Value = ""

	delete(kv.pending, key)
	kv.cond.Broadcast()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.ClientId*1000000 + int64(args.Seq) // Unique key for this RPC

	for kv.pending[key] {
		kv.cond.Wait()
	}

	if seq, exists := kv.requestCache[args.ClientId]; exists && args.Seq <= seq {
		lastIndex := strings.LastIndex(kv.kv[args.Key], args.Value)
		if lastIndex != -1 {
			reply.Value = kv.kv[args.Key][:lastIndex]
		} else {
			reply.Value = kv.kv[args.Key]
		}
		return
	}

	kv.pending[key] = true

	old := kv.kv[args.Key]
	if old == "" {
		kv.kv[args.Key] = args.Value
	} else {
		kv.kv[args.Key] += args.Value
	}

	kv.requestCache[args.ClientId] = args.Seq
	reply.Value = old

	delete(kv.pending, key)
	kv.cond.Broadcast()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.requestCache = make(map[int64]int)
	kv.pending = make(map[int64]bool)
	kv.cond = sync.NewCond(&kv.mu)

	return kv
}
