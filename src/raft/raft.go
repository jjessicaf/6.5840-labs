package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

var debugMode = false // Set to true to enable debug logging
var debug3D = false   // Debug flag for part 3D

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int

const (
	Leader    Status = iota // 0
	Candidate               // 1
	Follower                // 2
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int        // Latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int        // CandidateId that received vote in current term (or null if none)
	Log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile states
	commitIndex   int // Index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied   int // Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastHeartbeat time.Time

	status     Status
	applyCh    chan ApplyMsg
	nextIndex  []int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	snapshot          []byte // The current snapshot of Raft's persistent state
	lastIncludedIndex int    // Index of last entry included in snapshot
	lastIncludedTerm  int    // Term of last entry included in snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.CurrentTerm
	isleader = rf.status == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// This function should always be called when holding a rf lock
	// Serialize the Raft state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil ||
		e.Encode(rf.lastIncludedTerm) != nil {
		log.Fatal("Failed to encode state")
		return
	}

	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var entries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("Failed to decode persisted state")
		return
	}

	// Restore the persistent state
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Log = entries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	// Update volatile state based on restored state
	if rf.lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if debug3D {
		log.Printf("server %d - snapshot called: index = %d, last included index: %d, log: %v\n", rf.me, index, rf.lastIncludedIndex, rf.Log)
	}

	if index <= rf.lastIncludedIndex {
		return
	}

	physicalIndex := rf.toPhysicalIndex(index)
	if physicalIndex >= len(rf.Log) {
		// Snapshot beyond our log – keep only dummy
		rf.lastIncludedIndex = index
		if len(rf.Log) > 1 {
			rf.lastIncludedTerm = rf.termAt(len(rf.Log)-1, false)
		}
		rf.Log = []LogEntry{{Term: 0}}
		rf.snapshot = snapshot
		rf.persist()
		return
	}

	rf.lastIncludedTerm = rf.termAt(physicalIndex, false)
	rf.lastIncludedIndex = index

	newLog := make([]LogEntry, 1)
	newLog = append(newLog, rf.Log[physicalIndex+1:]...)

	rf.Log = newLog

	rf.snapshot = snapshot

	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// If candidate's term is higher, update our term and step down
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.status = Follower
		rf.persist()
		// Doesn't update lastHeartbeat since no new leader has been selected
	}

	/*
		Handles 5.4.1: A candidate must contact a majority of the cluster
		in order to be elected, which means that every committed
		entry must be present in at least one of those servers. If the
		candidate's log is at least as up-to-date as any other log
		in that majority (where "up-to-date" is defined precisely
		below), then it will hold all the committed entries.

		Raft determines which of two logs is more up-to-date
		by comparing the index and term of the last entries in the
		logs. If the logs have last entries with different terms, then
		the log with the later term is more up-to-date. If the logs
		end with the same term, then whichever log is longer is
		more up-to-date.
	*/

	var lastLogIndex int
	var lastLogTerm int
	if len(rf.Log) > 1 {
		lastLogIndex = rf.lastLogIndex()
		lastLogTerm = rf.lastLogIndexTerm()
	} else {
		lastLogIndex = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}

	upToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && upToDate {
		rf.VotedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect client
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower contained entry matchine prevLogIndex and prevLogTerm
	XTerm   int  // Term in the conflicting entry (if any)
	XIndex  int  // Index of first entry with that term (if any)
	XLen    int  // Index where the next entry should go if PrevLogIndex is beyond end of log
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	rf.stepDown(args.Term)

	rf.lastHeartbeat = time.Now()
	rf.status = Follower

	if !rf.matchPrevLog(args, reply) {
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it and append any new entries not already in the log
	for i, entry := range args.Entries {
		logicalLogIndex := args.PrevLogIndex + 1 + i // logical Raft index where this entry should go
		physicalLogIndex := rf.toPhysicalIndex(logicalLogIndex)
		if physicalLogIndex < 1 { // For safety
			continue
		}

		if physicalLogIndex >= len(rf.Log) {
			// Entry doesn't exist in log, append it and all remaining entries
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		} else if rf.termAt(physicalLogIndex, false) != entry.Term {
			// Term mismatch - truncate and append from here
			rf.Log = rf.Log[:physicalLogIndex]
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	// Update commit index if it has increased
	rf.updateFollowerCommitIndex(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) toPhysicalIndex(logical int) int {
	return logical - rf.lastIncludedIndex
}

// Caller must hold rf.mu.
func (rf *Raft) updateFollowerCommitIndex(leaderCommit, lastNewIndex int) {
	if leaderCommit > rf.commitIndex {
		lastNewEntryIndex := lastNewIndex
		if leaderCommit < lastNewEntryIndex {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
	}
}

// If the arguments don't match the log, return false.
// Caller must hold rf.mu.
func (rf *Raft) matchPrevLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	lastLogIndex := rf.lastLogIndex()

	// Case 1: Leader is referring to an entry beyond the end of our log
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.XLen = lastLogIndex + 1 // the index where the next entry should go
		return false
	}

	if args.PrevLogIndex == rf.lastIncludedIndex {
		// PrevLogIndex points to the last included entry in snapshot
		if rf.lastIncludedTerm != args.PrevLogTerm {
			if debugMode {
				log.Printf("server %d in appendEntries: FAIL - snapshot term= %d, prevlogterm= %d", rf.me, rf.lastIncludedTerm, args.PrevLogTerm)
			}

			reply.Success = false
			reply.Term = rf.CurrentTerm
			reply.XTerm = rf.lastIncludedTerm
			reply.XIndex = rf.lastIncludedIndex
			return false
		}
		return true
	}

	term := rf.termAt(args.PrevLogIndex, true)
	if term != args.PrevLogTerm {
		if debugMode {
			log.Printf("server %d in appendEntries: FAIL - local term= %d, prevlogterm= %d", rf.me, term, args.PrevLogTerm)
		}

		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.XTerm = term

		// Find first index of this term in the conflicting run
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex &&
			rf.termAt(i, true) == term {
			i--
		}

		reply.XIndex = i + 1 // first index of the conflicting term
		return false
	}

	return true
}

func (rf *Raft) termAt(index int, logical bool) int {
	if logical {
		return rf.Log[rf.toPhysicalIndex(index)].Term
	}

	return rf.Log[index].Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if debug3D {
		log.Printf("server %d in InstallSnapshot: args.lastincludedindex= %d, args.lastincludedterm= %d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	}

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		return
	}

	// Make sure the current server is a follower since it is receiving an InstallSnapshot
	rf.CurrentTerm = args.Term
	rf.VotedFor = -1
	rf.status = Follower
	rf.lastHeartbeat = time.Now()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		return
	}

	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.snapshot = args.Data

	//  If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	appendStart := -1
	for i := 1; i < len(rf.Log); i++ {
		logicalIndex := i + rf.lastIncludedIndex
		if logicalIndex == args.LastIncludedIndex && rf.termAt(i, false) == args.LastIncludedTerm {
			appendStart = i + 1
			break
		}
	}
	if appendStart > 0 {
		rf.Log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.Log[appendStart:]...)
	} else { //  Discard the entire log
		rf.Log = []LogEntry{{Term: args.LastIncludedTerm}} // dummy at index 0
	}

	// Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persist()

	go func(data []byte, index, term int) {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotIndex: index,
			SnapshotTerm:  term,
		}
	}(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if the server is the leader
	if rf.status != Leader {
		return -1, rf.CurrentTerm, false
	}

	// Append the command to the leader's log
	entry := LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	}
	rf.Log = append(rf.Log, entry)
	rf.persist()

	index = rf.lastLogIndex()
	term = rf.CurrentTerm

	if debugMode || debug3D {
		log.Printf("server %d: Starting command %v at index %d\n", rf.me, command, index)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.status = Follower
	rf.VotedFor = -1
	rf.persist()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// This function checks to see if an election for a new leader needs to occur
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)

		// Followers wait for randomized election timeouts
		randomDuration := 300 + (rand.Int31() % 151)
		timeout := time.Duration(randomDuration) * time.Millisecond
		time.Sleep(timeout)

		rf.mu.Lock()
		status := rf.status
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()

		if status != Leader &&
			time.Since(lastHeartbeat) >= timeout &&
			len(rf.peers) > 1 {
			rf.startElection()
		}
	}

	rf.mu.Lock()
	rf.status = Follower
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.status = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.lastHeartbeat = time.Now()
	currentTerm := rf.CurrentTerm // Store current term for consistency
	rf.persist()

	votes := 1
	lastLogTerm := rf.lastLogIndexTerm()
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.handleRequestVote(peer, args, &votes)
		}
	}
}

func (rf *Raft) handleRequestVote(peer int, args RequestVoteArgs, votes *int) {
	reply := RequestVoteReply{}
	if !rf.sendRequestVote(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm != args.Term || rf.status != Candidate {
		return // stale reply or no longer candidate
	}

	if rf.stepDown(reply.Term) {
		return
	}

	if !reply.VoteGranted {
		return
	}

	*votes++
	if *votes > len(rf.peers)/2 {
		rf.becomeLeader()
	}
}

// stepDown transitions this server to follower at newTerm.
// Caller must hold rf.mu.
func (rf *Raft) stepDown(newTerm int) bool {
	if newTerm <= rf.CurrentTerm {
		return false
	}

	rf.CurrentTerm = newTerm
	rf.status = Follower
	rf.VotedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.persist()

	return true
}

// Caller must hold rf.mu.
func (rf *Raft) lastLogIndexTerm() int {
	if len(rf.Log) > 1 {
		return rf.Log[len(rf.Log)-1].Term
	}

	return rf.lastIncludedTerm
}

// Returns the last logical index. Caller must hold rf.mu.
func (rf *Raft) lastLogIndex() int {
	return len(rf.Log) - 1 + rf.lastIncludedIndex
}

// Check if we can update commitIndex.
// Find the highest N such that a majority of matchIndex[i] ≥ N and log[N].term == currentTerm.
// Caller must hold rf.mu.
func (rf *Raft) updateLeaderCommitIndex() {
	for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
		physicalIndex := rf.toPhysicalIndex(N)
		if physicalIndex > 0 && rf.termAt(physicalIndex, false) == rf.CurrentTerm {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				if debugMode {
					log.Printf("handleSendAppendEntries: new commit index= %d", rf.commitIndex)
				}
				break
			}
		}
	}
}

func (rf *Raft) handleSendAppendEntries(peer int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.status != Leader || rf.CurrentTerm != args.Term {
			return
		}

		if rf.stepDown(reply.Term) {
			return
		}

		if reply.Success {
			if debugMode {
				log.Printf("server %d in handleSendAppendEntries: AppendEntries succeeded for peer %d: prevLogIndex %d\n", rf.me, peer, args.PrevLogIndex)
			}

			// Update nextIndex and matchIndex for the follower
			// rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			// rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.updateMatchAndNextIndex(peer, args.PrevLogIndex+len(args.Entries))

			if debugMode {
				log.Printf("handleSendAppendEntries: in here for peer %d: match idx= %d, next index= %d", peer, rf.matchIndex[peer], rf.nextIndex[peer])
			}

			rf.updateLeaderCommitIndex()
		} else {
			if debugMode {
				log.Printf("server %d in handleSendAppendEntries: AppendEntries failed for peer %d: prevLogIndex= %d, new nextIndex= %d", rf.me, peer, args.PrevLogIndex, rf.nextIndex[peer])
			}

			// The below is an optimized version of:
			// If append failed because of log inconsistency, decrement nextIndex and retry
			// This "walking back" approach guarantees that eventually the leader will find
			// the point where the logs match (keep going back if conflicting)
			if reply.XLen != 0 {
				// Case 3: follower's log is too short
				rf.nextIndex[peer] = reply.XLen
			} else {
				// Look for XTerm in leader log
				index := rf.findLastIndexOfTerm(reply.XTerm)

				if index == -1 {
					// Case 1: leader doesn't have XTerm
					rf.nextIndex[peer] = reply.XIndex
				} else {
					// Case 2: leader has XTerm
					rf.nextIndex[peer] = index + 1
				}
			}
		}
	}
}

func (rf *Raft) findLastIndexOfTerm(term int) int {
	index := -1
	for i := 1; i < len(rf.Log); i++ {
		if rf.termAt(i, false) == term {
			index = i + rf.lastIncludedIndex
		}
	}

	if index == -1 && term == rf.lastIncludedTerm {
		return rf.lastIncludedIndex
	}

	return index
}

// handleInstallSnapshot sends InstallSnapshot if peer is behind
func (rf *Raft) handleInstallSnapshot(peer int) {
	rf.mu.Lock()
	if rf.status != Leader { // Only leaders can send snapshots
		rf.mu.Unlock()
		return
	}

	logicalNextIndex := rf.nextIndex[peer]
	if logicalNextIndex > rf.lastIncludedIndex { // This peer doesn't need a snapshot
		rf.mu.Unlock()
		return
	}

	if debugMode {
		log.Printf("server %d in handleInstallSnapshot to peer %d: send install snapshot called", rf.me, peer)
	}

	data := append([]byte(nil), rf.snapshot...)
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              data,
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check we're still leader in same term
	if rf.status != Leader || rf.CurrentTerm != args.Term {
		return
	}

	if rf.stepDown(reply.Term) {
		return
	}

	rf.updateMatchAndNextIndex(peer, args.LastIncludedIndex)

	if debug3D {
		log.Printf("handleInstallSnapshot: in here for peer %d: match idx= %d, next index= %d", peer, rf.matchIndex[peer], rf.nextIndex[peer])
	}
}

// Caller must hold rf.mu.
func (rf *Raft) updateMatchAndNextIndex(peer int, newMatchIndex int) {
	if newMatchIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = newMatchIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
}

func (rf *Raft) sendHeartbeat() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				term := rf.CurrentTerm

				if rf.status != Leader { // Only leader should send heartbeat
					rf.mu.Unlock()
					return
				}

				logicalNextIndex := rf.nextIndex[peer]

				// Check if the follower needs to install snapshot
				if logicalNextIndex <= rf.lastIncludedIndex {
					rf.mu.Unlock()

					rf.handleInstallSnapshot(peer)
					return
				}

				logicalPrevLogIndex := logicalNextIndex - 1
				prevTerm := rf.getPrevLogTerm(logicalPrevLogIndex)

				if debugMode {
					log.Printf("server %d in sendHeartbeat to peer %d: prevlogindex = %d, logical prevlogindex = %d, log length (including dummy) = %d, physical next index = %d, lastincludedindex = %d", rf.me, peer, logicalPrevLogIndex-rf.lastIncludedIndex, logicalPrevLogIndex, len(rf.Log), logicalNextIndex-rf.lastIncludedIndex, rf.lastIncludedIndex)
				}

				// Create a separate args struct for this peer to avoid race conditions
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}

				args.PrevLogIndex = logicalPrevLogIndex
				args.PrevLogTerm = prevTerm

				// Log replication
				logLength := len(rf.Log)
				physicalNextIndex := rf.toPhysicalIndex(logicalNextIndex)
				// Make a copy of the log entries to avoid concurrent modification during RPC
				entries := make([]LogEntry, logLength-physicalNextIndex)
				copy(entries, rf.Log[physicalNextIndex:])
				args.Entries = entries

				rf.mu.Unlock()

				rf.handleSendAppendEntries(peer, &args)
			}(peer)
		}
	}
}

// Use logical indices since with snapshotting, the physical indices can cause prevLogIndex to be 0,
// which will lead to invalid bounds with the physical indices
// Caller must hold rf.mu.
func (rf *Raft) getPrevLogTerm(logicalPrevLogIndex int) int {
	if logicalPrevLogIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	return rf.termAt(logicalPrevLogIndex, true)
}

// Caller must hold rf.mu.
func (rf *Raft) becomeLeader() {
	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
	if debugMode || debug3D {
		log.Printf("server %d became leader: term %d\n", rf.me, rf.CurrentTerm)
	}

	numPeers := len(rf.peers)
	rf.status = Leader
	rf.nextIndex = make([]int, numPeers)
	rf.matchIndex = make([]int, numPeers)

	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.Log)
		rf.matchIndex[i] = rf.lastIncludedIndex // gauranteed to be replicated up to the last index included in the snapshot
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		if rf.status == Leader {
			// Leaders send heartbeats at fixed shorter intervals
			rf.sendHeartbeat()
		}

		time.Sleep(100 * time.Millisecond)
	}
	rf.mu.Lock()
	rf.status = Follower
	rf.mu.Unlock()
}

// This function should be started as a goroutine when initializing your Raft instance
func (rf *Raft) applyCommitted() {
	for !rf.killed() {
		// Sleep a bit to avoid busy waiting
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}

		// Check if there are new entries to apply
		var entriesToApply []ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			if debugMode {
				log.Printf("server %d applying committed entries: term %d\n", rf.me, rf.CurrentTerm)
			}

			physicalIndex := rf.toPhysicalIndex(rf.lastApplied)
			if physicalIndex > 0 && physicalIndex < len(rf.Log) {
				entriesToApply = append(entriesToApply, ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[physicalIndex].Command,
					CommandIndex: rf.lastApplied,
				})
			}
		}
		rf.mu.Unlock()

		// Send without holding lock since channels can block
		for _, msg := range entriesToApply {
			rf.applyCh <- msg
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	if debug3D {
		log.Printf("Starting new server %d", me)
	}

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogEntry{{Term: 0}}
	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0 // Index of last entry included in snapshot
	rf.lastIncludedTerm = -1 // Term of last entry included in snapshot

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 1 // Start from index 1
		rf.matchIndex[i] = 0
	}
	rf.status = Follower // When servers start up, they begin as followers
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine for leader to send heartbeats
	go rf.heartbeat()

	// start goroutine to check for newly committed entries
	go rf.applyCommitted()

	return rf
}
