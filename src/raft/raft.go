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
	"fmt"
	"sync"
	"time"
)
import "labrpc"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister // currentTerm, voteFor, log[]
	me        int        // index into peers[]

	// TODO: Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// TODO: Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// TODO: Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// TODO: Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// TODO: Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// TODO: Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: Your code here.
	// receiver
	// if args.term < rf.term
	// 	return false
	// rf.term = max(rf.term, args.term)
	//
	// if (rf.voteFor == nil || rf.voteFor == args.candidateId)
	//  	&& candidate's log is at least as up-to-date as receiver's log
	// 	(up-to-date means:
	//	1. args.lastLogIndex > rf.lastLogIndex
	// 	2. args.lastLogIndex == rf.lastLogIndex && args.lastLogTerm >= rf.lastLogTerm
	//	}
	// 	return true
	fmt.Printf("server[%d] receive RequestVote RPC: term=%d, candidateId=%d, lastLogIndex=%d, lastLogTerm=%d\n",
		rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	reply.Term, _ = rf.GetState()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs
// AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // leader's id
	PrevLogIndex int // index of log entry immediately preceding new ones
	ProvLogTerm  int // term of prevLogIndex entry
	// TODO: entries' type
	Entries      []int // log entries to store (empty for heartbeat)
	LeaderCommit int   // leader's commitIndex
}

// AppendEntriesReply
// result of AppendEntries RPC
type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success int // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args RequestVoteArgs, reply *RequestVoteReply) {
	// TODO
	// if args.term < rf.currentTerm
	// 	return true
	// if rf.log[args.PrevLogIndex].term != args.PrevLogTerm
	// 	return false
	// if an existing entry conflicts with a new one (same index, different term)
	// 	delete rf.log[conflict index:-1]
	// Append any new entries not already in rf.log[]
	// if args.LeaderCommit > rf.commitIndex
	// 	rf.commitIndex = min(args.LeaderCommit, index of last new entry
}

// sendAppendEntries
// send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// TODO
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// TODO: Your code here, if desired.
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// TODO: Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// TODO:
	// start a goroutine: listening to other peers,
	// if no heartbeats for a while, then starts an election

	// timeOut = xx ms
	// goroutine1: timer
	// 	initTime = timer.getCurrentTime()
	// 	while (timer.getCurrentTime() - initTime < timeOut) {}
	// 	return "time out"
	//
	// goroutine2: main
	// 	listening to other peers
	// 	if heard, reset timer, handle RPC
	// 	if "time out", start election

	go func() {
		timeOut := time.Millisecond * 200
		timer := time.NewTimer(timeOut) // TODO: random?
		go func() {
			<-timer.C
			fmt.Printf("%d passed, server[%d] hear nothing\n", timeOut, rf.me)
			fmt.Printf("server[%d] start election\n", rf.me)
		}()
		fmt.Printf("server[%d] is waiting\n", rf.me)
		receiver := 1
		requestVoteArgs := RequestVoteArgs{Term: 1, CandidateId: 1, LastLogIndex: 1, LastLogTerm: 1}
		requestVoteReply := RequestVoteReply{}
		rf.sendRequestVote(receiver, requestVoteArgs, &requestVoteReply)
	}()

	return rf
}
