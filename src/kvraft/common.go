package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	PutOp          = "Put"
	AppendOp       = "Append"
	GetOp          = "Get"
)

type Err string

type OpType string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq      int // Latest serial number for commands from this client
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq      int // Latest serial number for commands from this client
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
