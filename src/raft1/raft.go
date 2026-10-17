package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	//3A
	role          string      // 该服务的角色，follewer、candidate、leader
	currentTerm   int         // 当前任期号
	votedFor      int         // 当前任期内投票给了谁
	electionTimer *time.Timer // 选举计时器

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == "leader"
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term { // 如果请求的任期号大于当前任期号，说明自己落后了
		rf.currentTerm = args.Term
		rf.role = "follower"
		rf.votedFor = -1
		reply.VoteGranted = false
	}
	if rf.currentTerm == args.Term { // 如果请求的任期号等于当前任期号
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // 如果自己还没有投票，或者已经投票给了请求者
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			ms := 400 + (rand.Int63() % 300)                             // 400-700ms
			rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器
		} else { // 如果自己已经投票给了其他人
			reply.VoteGranted = false
		}
	} else { // 如果请求的任期号小于当前任期号
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
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

type HeartBeatArgs struct {
	Term int
}

type HeartBeatReply struct {
	Term int
}

func (rf *Raft) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.role = "follower"

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	ms := 400 + (rand.Int63() % 300)                             // 400-700ms
	rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器
}

func (rf *Raft) sendHeartBeat(term int) {
	for {
		rf.mu.Lock()

		if rf.role != "leader" || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		args := &HeartBeatArgs{
			Term: rf.currentTerm,
		}

		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			reply := &HeartBeatReply{}
			go func(server int, args *HeartBeatArgs) {
				if !rf.peers[server].Call("Raft.HeartBeat", args, reply) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = "follower"
					rf.votedFor = -1
					ms := 400 + (rand.Int63() % 300)
					rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器
				}
			}(i, args)
		}
		time.Sleep(100 * time.Millisecond) // 发送心跳间隔
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()

	if rf.role == "leader" { // 如果自己是leader，就不需要发起选举
		rf.mu.Unlock()
		return
	}

	rf.role = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer.Reset(time.Duration(400+rand.Int63()%300) * time.Millisecond)
	tickets := 1
	term := rf.currentTerm // 后续在for中其他rf可能会改term

	if tickets > len(rf.peers)/2 { // 单节点情况
		rf.role = "leader"
		go rf.sendHeartBeat(rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
		}
		reply := &RequestVoteReply{}

		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {

			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if ok {

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = "follower"
					rf.votedFor = -1
					ms := 400 + (rand.Int63() % 300)
					rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器
				}

				if rf.currentTerm != term || rf.role != "candidate" {
					return
				}

				if reply.VoteGranted {

					tickets++
					if tickets > len(rf.peers)/2 {
						rf.role = "leader"
						go rf.sendHeartBeat(rf.currentTerm)
						return
					}
				}

			}
		}(i, args, reply)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for { // true有啥用……

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		<-rf.electionTimer.C
		rf.StartElection()
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	ms := 400 + (rand.Int63() % 300) // 400-700ms
	rf.electionTimer = time.NewTimer(time.Duration(ms) * time.Millisecond)
	// Your initialization code here (3A, 3B, 3C).
	//3A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = "follower"

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
