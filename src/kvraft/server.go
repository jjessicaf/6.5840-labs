package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
const timeout = 1 * time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpType
	Key      string
	Value    string
	Seq      int
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv           map[string]string
	notify       map[int]chan Op // Map command ID to notify channel for each command
	requestCache map[int64]int   // Client ID to last sequence number
	lastApplied  int
	persister    *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// Start agreement on the next command to be appended to Raft's log
	// Start will check if the current server is the leader
	cmd := Op{
		OpType:   GetOp,
		Key:      args.Key,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(
		"server %d START GET index=%d term=%d client=%d seq=%d",
		kv.me, index, term, args.ClientId, args.Seq,
	)

	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case applied := <-ch:
		DPrintf(
			"server %d WAKE GET index=%d wanted=(%d,%d) got=(%d,%d)",
			kv.me,
			index,
			args.ClientId,
			args.Seq,
			applied.ClientId,
			applied.Seq,
		)

		if applied.ClientId == args.ClientId &&
			applied.Seq == args.Seq {
			if applied.Value == ErrNoKey {
				reply.Err = ErrNoKey
			} else {
				reply.Value = applied.Value
				reply.Err = OK
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(timeout):
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.notify, index) // Safe deletion under lock
		kv.mu.Unlock()
		DPrintf("server %d notify size=%d", kv.me, len(kv.notify))
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.handlePutAppend(PutOp, args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.handlePutAppend(AppendOp, args, reply)
}

func (kv *KVServer) handlePutAppend(opType OpType, args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// Start agreement on the next command to be appended to Raft's log
	// Start will check if the current server is the leader
	cmd := Op{
		OpType:   opType,
		Key:      args.Key,
		Value:    args.Value,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}

	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(
		"server %d START %s index=%d term=%d client=%d seq=%d",
		kv.me, opType, index, term, args.ClientId, args.Seq,
	)

	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case applied := <-ch:
		DPrintf(
			"server %d WAKE %s index=%d wanted=(%d,%d) got=(%d,%d)",
			kv.me,
			opType,
			index,
			args.ClientId,
			args.Seq,
			applied.ClientId,
			applied.Seq,
		)

		if applied.ClientId == args.ClientId &&
			applied.Seq == args.Seq {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(timeout):
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.notify, index) // Safe deletion under lock
		kv.mu.Unlock()
		DPrintf("server %d notify size=%d", kv.me, len(kv.notify))
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for idx := range kv.notify {
		delete(kv.notify, idx)
	}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Updates the cache and the waiting RPCs whenever a new committed command is received
func (kv *KVServer) update() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		if msg.SnapshotValid && msg.SnapshotIndex > kv.lastApplied {
			kv.readSnapshot(msg.Snapshot) // restore kv.kv + kv.requestCache from bytes
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
			continue
		}

		if !msg.CommandValid {
			kv.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)

		DPrintf(
			"server %d APPLY index=%d client=%d seq=%d",
			kv.me, msg.CommandIndex, op.ClientId, op.Seq,
		)

		if !kv.duplicate(op.ClientId, op.Seq) {
			kv.apply(&op)
		} else if op.OpType == GetOp {
			op.Value = kv.get(op.Key)
		}

		kv.lastApplied = msg.CommandIndex

		// Handle snapshot after all states are updated
		if kv.shouldSnapshot() {
			kv.snapshot()
		}

		ch, ok := kv.notify[msg.CommandIndex]
		delete(kv.notify, msg.CommandIndex)
		kv.mu.Unlock()

		if ok {
			select {
			case ch <- op:
			default:
			}
		}
	}
}

// Apply the op to the server's local cache.
// Should only be done if the op has been committed by Raft.
// Must be called while lock is held.
func (kv *KVServer) apply(op *Op) {
	switch op.OpType {
	case PutOp:
		kv.kv[op.Key] = op.Value
	case AppendOp:
		kv.kv[op.Key] = kv.kv[op.Key] + op.Value
	case GetOp:
		op.Value = kv.get(op.Key)
	}

	kv.requestCache[op.ClientId] = op.Seq
}

// Must be called while lock is held.
func (kv *KVServer) get(key string) string {
	if val, ok := kv.kv[key]; ok {
		return val
	}

	return ErrNoKey
}

// Checks if a op is a duplicate.
// Must be called while lock is held.
func (kv *KVServer) duplicate(clientId int64, currentSeq int) bool {
	seq, exists := kv.requestCache[clientId]
	DPrintf("server %d - checking duplicate: clientID: %d, currentSeq: %d, exists: %v, request cache seq: %d", kv.me, clientId, currentSeq, exists, seq)
	return exists && currentSeq <= seq
}

func (kv *KVServer) shouldSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kv)
	e.Encode(kv.requestCache)
	e.Encode(kv.lastApplied)
	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}

// Must be called while lock is held
func (kv *KVServer) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var requestCache map[int64]int
	var lastApplied int

	if d.Decode(&kvMap) != nil ||
		d.Decode(&requestCache) != nil ||
		d.Decode(&lastApplied) != nil {
		// Should not happen if snapshot was written correctly.
		DPrintf("server %d FAILED to decode snapshot", kv.me)
		return
	}

	// update local vars
	kv.kv = kvMap
	kv.requestCache = requestCache
	kv.lastApplied = lastApplied
}

// Returns the channel to be used for notification for waiting RPCs.
// Must be called while lock is held.
func (kv *KVServer) getNotifyCh(index int) chan Op {
	if ch, ok := kv.notify[index]; ok {
		return ch
	}

	ch := make(chan Op, 1)
	kv.notify[index] = ch

	DPrintf("server %d create notify index=%d size=%d",
		kv.me, index, len(kv.notify))

	return ch
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.requestCache = make(map[int64]int)
	kv.notify = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.readSnapshot(kv.persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, kv.persister, kv.applyCh)

	// You may need initialization code here.
	go kv.update()

	return kv
}
