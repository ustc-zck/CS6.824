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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"github.com/ngaut/log"
)

const (
	Leader    int32 = 0
	Follower  int32 = 1
	Candidate int32 = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term int
	Op   interface{}
}

// A Go object implementing a single Raft peer.

type SnapShot struct {
	Data  []byte
	Index int
	Term  int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// role， master, follower, candidata
	role atomic.Int32

	// persistent state
	currentTerm int // current term
	votedFor    int // voted index into peers[]

	logs []*Log // logs

	// last received append entries time
	lastReceivedAppendEntriesTime int64
	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// apply channel for test
	applyChan chan ApplyMsg

	// snap shot
	snapShot SnapShot
}

func (rf *Raft) LastLogIndex() int64 {
	return int64(len(rf.logs)) - 1
}

func (rf *Raft) LastLogTerm() int {
	lastLogIndex := rf.LastLogIndex()
	return rf.logs[lastLogIndex].Term
}

func (rf *Raft) PrevLogIndex(server int) int {
	//return rf.matchIndex[index]
	return rf.nextIndex[server] - 1
}

func (rf *Raft) PrevLogTerm(server int) int {
	//return rf.logs[rf.matchIndex[index]-1].term
	return rf.logs[rf.nextIndex[server]-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = (rf.role.Load() == Leader)

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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.snapShot.Index) != nil || e.Encode(rf.snapShot.Term) != nil {
		panic("failed to encode some fields")
	}

	raftstate := w.Bytes()
	// warning: since the persister provides a very simple interface, there's no way to not persist
	// raftstate and snapshot together while ensures they're in sync.

	// TODO, snapshot
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil || d.Decode(&rf.snapShot.Index) != nil || d.Decode(&rf.snapShot.Term) != nil {
		panic("failed to decode some fields")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int // index into peers
	LastLogIndex int64
	LastLogTerm  int
	CommitIndex  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.persist()

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//update current term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role.Store(Follower)
	}

	log.Infof("rf %d lastlogterm %d lastlogindex %d, args lastlogterm %d, args lastlogindex %d", rf.me, rf.LastLogTerm(), rf.LastLogIndex(), args.LastLogTerm, args.LastLogTerm)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.CommitIndex >= rf.commitIndex) &&
		(args.LastLogTerm > rf.LastLogTerm() || (args.LastLogIndex >= rf.LastLogIndex() && args.LastLogTerm == rf.LastLogTerm())) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.role.Store(Follower)

		// if voted for other node, dont compaign in some time
		rf.lastReceivedAppendEntriesTime = time.Now().UnixMilli()
		return
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	defer rf.persist()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastReceivedAppendEntriesTime = time.Now().UnixMilli()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//log.Infof("leader %d term is %d, raft %d term is %d", args.LeaderId, args.Term, rf.me, rf.currentTerm)

	log.Infof("raft %d receive append entries, term %d, leaderid %d, prevlogindex %d, prevlodterm %d, entries %v, leader commit id %d",
		rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

		if rf.role.Load() == Leader {
			log.Infof("raft %d turn into follower from leader", rf.me)
		}

		//log.Infof("raft %d got larger term from %d", rf.me, args.LeaderId)
		rf.role.Store(Follower)
		rf.votedFor = -1
	}

	// contain := false
	// for index, log := range rf.logs {
	// 	if index == int(args.PrevLogIndex) && log.term == args.PrevLogTerm {
	// 		contain = true
	// 	}
	// }

	// if !contain {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }

	// log.Infof("raft %d, log length %d, prevlogindex %d, prevlogterm %d", rf.me, len(rf.logs), args.PrevLogIndex, args.PrevLogTerm)
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// remove conflict log entry
	// log.Infof("raft %d remove conflict log entries", rf.me)
	var i int
	for i = 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i < len(rf.logs) && rf.logs[args.PrevLogIndex+i].Term != args.Entries[i].Term {
			// conflict
			// remove conclift
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			break
		}
	}

	// append log entries, duplicated items will be
	// log.Infof("raft %d append new entries, entry length %d", rf.me, len(args.Entries))
	// log.Infof("entries length is %d", len(args.Entries))
	for i, entry := range args.Entries {
		if args.PrevLogIndex+i+1 >= len(rf.logs) {
			log.Infof("raft follower %d appends new entries %+v", rf.me, entry)
			rf.logs = append(rf.logs, entry)
		} else {
			log.Infof("raft follower %d exists old entries", rf.me)
			rf.logs[args.PrevLogIndex+i+1] = entry
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

// 命令入口/流量入口
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	//isLeader := true
	isLeader := false

	isLeader = (rf.role.Load() == Leader)

	if isLeader {
		// Your code here (2B).
		rf.mu.Lock()
		index = len(rf.logs)
		term = rf.currentTerm
		log.Infof("raft %d receive new command %+v, term is %d", rf.me, command, rf.currentTerm)
		rf.logs = append(rf.logs, &Log{Term: term, Op: command})
		rf.mu.Unlock()
	}

	// for _, log := range rf.logs {
	// 	fmt.Println(log.Op)
	// }

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) chonseAsLeader() {
	rf.role.Store(Leader)

	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}

	rf.persist()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// log.Infof("raft %d role is %d", rf.me, rf.role.Load())
		if time.Now().UnixMilli()-rf.lastReceivedAppendEntriesTime > 1000 && rf.role.Load() != Leader {
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.role.Store(Candidate)

			log.Infof("raft %d is candidata, term is %d", rf.me, rf.currentTerm)
			// request vote from other peers

			var cnt atomic.Int64
			cnt.Store(1)
			var sw sync.WaitGroup
			for index, _ := range rf.peers {
				if index == rf.me {
					continue
				}
				sw.Add(1)
				go func(index_ int) {
					defer sw.Done()
					arg := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.LastLogIndex(),
						LastLogTerm:  rf.LastLogTerm(),
						CommitIndex:  rf.commitIndex,
					}
					reply := RequestVoteReply{}
					// log.Infof("raft %d start to request vote, args %+v", rf.me, arg)
					if ok := rf.sendRequestVote(index_, &arg, &reply); ok {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.role.Store(Follower)
							return
						}
						//log.Infof("raft %d start to request vote, reply %+v", rf.me, reply)
						if reply.VoteGranted {
							cnt.Add(1)
							// log.Infof("raft %d got vote from %d, cnt is %d", rf.me, index_, cnt.Load())
							// fix
							if rf.role.Load() == Candidate && int(cnt.Load()) > len(rf.peers)/2 {
								log.Infof("raft %d chosen as leader", rf.me)
								rf.chonseAsLeader()
							}
							return
						}
					}
				}(index)
			}
			sw.Wait()
			// log.Infof("raft %d got votes num %d", rf.me, cnt.Load())
			// if rf.role.Load() == Candidate && int(cnt.Load()) > len(rf.peers)/2 {
			// 	log.Infof("raft %d chosen as leader", rf.me)
			// 	rf.chonseAsLeader()
			// }
			rf.persist()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// ledaer append entries to followers
func (rf *Raft) ticker2() {

	for rf.killed() == false {
		if rf.role.Load() == Leader {

			// append entries
			// var sw sync.WaitGroup
			var cnt atomic.Int32
			cnt.Store(0)
			for index, _ := range rf.peers {
				if index == rf.me {
					continue
				}
				//sw.Add(1)
				go func(index_ int) {
					//defer sw.Done()

					entries := []*Log{}

					// send one item
					// TODO, send more items once
					if rf.nextIndex[index_] < len(rf.logs) {
						//entries = append(entries, rf.logs[rf.nextIndex[index_]])
						entries = rf.logs[rf.nextIndex[index_]:]
					}

					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.PrevLogIndex(index_),
						PrevLogTerm:  rf.PrevLogTerm(index_),
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					// todo, set timeout for AppendEntries rpc
					if ok := rf.sendAppendEntries(index_, &args, &reply); ok {
						log.Infof("raft leader %d ping to server %d ok, args term %d, leader id %d, prevlogindex %d, prevlogterm %d,  entries %+v, leader commit %d",
							rf.me, index_, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
						// update leader elements
						if reply.Success {
							// fix heartbeat ping
							if len(args.Entries) > 0 {
								if rf.nextIndex[index_] < len(rf.logs) {
									rf.nextIndex[index_] = len(rf.logs)
									rf.matchIndex[index_] = rf.nextIndex[index_] - 1
								}
								cnt.Add(1)
							}
						} else {
							// 失败原因分为冲突或者更大的term
							// got larger term
							// log.Infof("raft %d refused to append entries", index_)
							if reply.Term > rf.currentTerm {
								// 避免立刻发起选主行为
								// log.Infof("raft %d got larger term from %d", rf.me, index_)
								rf.lastReceivedAppendEntriesTime = time.Now().UnixMilli()
								log.Infof("raft %d turns into follower", rf.me)
								rf.role.Store(Follower)
								rf.currentTerm = reply.Term
								rf.votedFor = -1
							} else {
								// prevlogindex prelogterm conflict
								rf.nextIndex[index_] -= 1
							}
						}
					} else {
						//log.Infof("leader %d ping to peer %d error", rf.me, index_)
					}
				}(index)
			}

			// sw.Wait()
			time.Sleep(100 * time.Millisecond)
			// successfully append new entries
			// log.Infof("leader %d append entries got cnt %d", rf.me, cnt.Load())
			if int(cnt.Load()+1) > len(rf.peers)/2 {
				rf.commitIndex += 1
			}

			var i int

			start := rf.commitIndex
			for i = start; i < len(rf.logs); i++ {
				cnt := 0
				for _, index := range rf.matchIndex {
					if index >= i {
						cnt += 1
					}
				}
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = i
				}
			}
			rf.persist()
		}
	}
}

func (rf *Raft) ticker3() {
	for rf.killed() == false {
		for i := rf.lastApplied + 1; i <= rf.commitIndex && i < len(rf.logs); i++ {
			// apply msg
			msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Op, CommandIndex: i}
			log.Infof("raft %d applys new msg %+v", rf.me, msg)
			rf.applyChan <- msg
			rf.lastApplied += 1
		}
		time.Sleep(10 * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role.Store(Follower)

	rf.lastReceivedAppendEntriesTime = time.Now().UnixMilli()

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	// first index is 1
	rf.logs = make([]*Log, 0)
	rf.logs = append(rf.logs, &Log{Term: 0})

	// committed log index
	rf.commitIndex = 0

	// last applied log index
	rf.lastApplied = 0

	// reinitialize after election
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// apply channel
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	if rf.persister.RaftStateSize() > 0 {
		rf.readPersist(persister.ReadRaftState())
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// start to append entries for leader
	go rf.ticker2()

	// applied ticker
	go rf.ticker3()

	return rf
}
