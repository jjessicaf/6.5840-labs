package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seq    int // Latest serial number for commands from this client
	leader int // Current known leader index
	id     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seq = 0
	ck.id = nrand()
	ck.leader = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		Seq:      ck.seq,
		ClientId: ck.id,
	}

	ck.seq++

	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		} else if ok && reply.Err == ErrNoKey {
			return ""
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 { // Only sleep after going through all the servers once
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Seq:      ck.seq,
		ClientId: ck.id,
	}

	ck.seq++

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			return
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 { // Only sleep after going through all the servers once
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
