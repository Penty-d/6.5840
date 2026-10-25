package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

	//3B
	logEntries  []LogEntry // 日志条目
	commitIndex int
	applyIndex  int
	matchIndex  []int
	nextIndex   []int
	applyChan   chan raftapi.ApplyMsg
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
func (rf *Raft) persist() { // 持锁
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logEntries) != nil {
		panic("encode error")
	}
	rf.persister.Save(w.Bytes(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) { // 持锁
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor int
	var entries []LogEntry // 消除一下 warning
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil {
		panic("decode error")
	}
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.logEntries = entries
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
	// 3A
	Term        int
	CandidateId int

	// 3B
	LastLogIndex int
	LastLogTerm  int
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

	reply.VoteGranted = false

	if rf.currentTerm < args.Term { // 如果请求的任期号大于当前任期号，说明自己落后了
		rf.currentTerm = args.Term
		rf.role = "follower"
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	if rf.currentTerm == args.Term { // 如果请求的任期号等于当前任期号

		lastLogIndex := len(rf.logEntries) - 1
		lastLogTerm := rf.logEntries[lastLogIndex].Term

		valid := (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) || args.LastLogTerm > lastLogTerm // 请求者的日志至少和自己一样新

		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && valid { // 如果自己还没有投票，或者已经投票给了请求者

			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.persist()
			ms := 400 + (rand.Int63() % 300)                             // 400-700ms
			rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器

		} // 如果自己已经投票给了其他人
	} // 如果请求的任期号小于当前任期号
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

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int //已经存在的日志索引
	PrevLogTerm  int //已经存在的日志任期
	Entries      []LogEntry
	LeaderCommit int //Leader允许的最大提交索引
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // 冲突日志的任期 3C中fail了，为了快速恢复添加的字段
	ConflictIndex int // 冲突日志的索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.role = "follower"

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	ms := 400 + (rand.Int63() % 300)                             // 400-700ms
	rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器

	if args.PrevLogIndex >= len(rf.logEntries) {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.logEntries) // 3C中fail了，为了快速恢复添加的字段
		return                                   // 没有该索引
	}

	if args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term {
		reply.ConflictTerm = rf.logEntries[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index > 0 && rf.logEntries[index-1].Term == reply.ConflictTerm {
			index--
		}
		reply.ConflictIndex = index
		return // 任期不同
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index >= len(rf.logEntries) { // 如果没有该索引，说明后续是新的日志，直接追加
			rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.logEntries[index].Term != entry.Term { // 如果任期不同，说明日志不一致，需要删除后续日志
			rf.logEntries = append(rf.logEntries[:index], args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	totalEntries := args.PrevLogIndex + len(args.Entries)
	newCommitIndex := min(args.LeaderCommit, totalEntries)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
	}

	reply.Success = true

}

func (rf *Raft) sendHeartBeat(term int) {
	for {
		for i := 0; i < len(rf.peers); i++ {

			if i == rf.me {
				continue
			}

			rf.mu.Lock()

			if rf.role != "leader" || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			index := rf.nextIndex[i]

			args := &AppendEntriesArgs{
				Term:         term,
				PrevLogIndex: index - 1,
				PrevLogTerm:  rf.logEntries[index-1].Term,
				Entries:      append([]LogEntry(nil), rf.logEntries[index:]...), // 深拷贝，防止后续修改
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			go func(server int, args *AppendEntriesArgs) {
				if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = "follower"
					rf.votedFor = -1
					rf.persist()
					ms := 400 + (rand.Int63() % 300)
					rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器
				}

				if rf.currentTerm != term || rf.role != "leader" {
					return
				}

				if reply.Success {
					rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries)) // 用max防止回退
					rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
					rf.VerifyCommitted()
				} else if rf.nextIndex[server] == args.PrevLogIndex+1 &&
					rf.nextIndex[server] > rf.matchIndex[server]+1 { //防nextindex被已经修改，且确保还有回退空间

					newNext := reply.ConflictIndex

					if reply.ConflictTerm != -1 {
						for i := len(rf.logEntries) - 1; i >= 1; i-- {
							if rf.logEntries[i].Term == reply.ConflictTerm {
								newNext = i + 1
								break
							}
						}
					}

					rf.nextIndex[server] = max(1, max(newNext, rf.matchIndex[server]+1))
				}
				/*else if rf.nextIndex[server] == args.PrevLogIndex+1 && rf.nextIndex[server] > rf.matchIndex[server]+1 { // 如果失败了，说明日志不一致，需要回退
					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1) // 防止回退到0
				}*/
			}(i, args)
		}
		time.Sleep(100 * time.Millisecond) // 发送心跳间隔
	}
}

func (rf *Raft) initLeaderProgress() {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logEntries)
	}
	rf.matchIndex[rf.me] = len(rf.logEntries) - 1
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
	rf.persist()
	rf.electionTimer.Reset(time.Duration(400+rand.Int63()%300) * time.Millisecond)
	tickets := 1
	term := rf.currentTerm // 后续在for中其他rf可能会改term

	if tickets > len(rf.peers)/2 { // 单节点情况
		rf.role = "leader"
		rf.initLeaderProgress()
		go rf.sendHeartBeat(rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	index := len(rf.logEntries) - 1

	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  rf.logEntries[index].Term,
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
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
					rf.persist()
					ms := 400 + (rand.Int63() % 300)
					rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond) // 重置选举计时器
				}

				if rf.currentTerm != term || rf.role != "candidate" {
					return
				}

				if reply.VoteGranted {

					tickets++
					if tickets > len(rf.peers)/2 {
						rf.initLeaderProgress()
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

type LogEntry struct {
	Command any
	Term    int
}

func (rf *Raft) VerifyCommitted() { //保证需要mulock，内部不再加锁
	for index := len(rf.logEntries) - 1; index > rf.commitIndex; index-- { // 倒找
		if rf.logEntries[index].Term != rf.currentTerm {
			continue
		}

		count := 0

		for _, match := range rf.matchIndex {
			if match >= index {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = index
			break
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	if rf.role != "leader" {
		isLeader = false
		return index, term, isLeader
	}

	index = len(rf.logEntries)
	rf.logEntries = append(rf.logEntries, LogEntry{Command: command, Term: term})
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.VerifyCommitted()

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

	//3B
	rf.logEntries = []LogEntry{{Term: 0}} // index =len (slice) 放一个空初始化，让index从1开始
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		if rf.commitIndex <= rf.applyIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		index := rf.applyIndex + 1

		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[index].Command,
			CommandIndex: index,
		}

		rf.mu.Unlock() // channel发送可能会阻塞，先解锁

		rf.applyChan <- applyMsg

		rf.mu.Lock()

		rf.applyIndex = index

		rf.mu.Unlock()
	}
}
