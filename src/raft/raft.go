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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// timeOut
const recvHeartbeatTimeOutL = 150
const recvHeartbeatTimeOutR = 300
const heartbeatInterval = 50 * time.Millisecond
const RPCTimeOut = time.Second
const networkFailRetryTimes int = 25

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// LogEntry
// An entry in Raft.log[].
type LogEntry struct {
	Term    int         // term when entry was received by the leader
	Command interface{} // command
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister // currentTerm, votedFor, log[] stored in disk?
	me        int        // index into peers[]

	currentTerm int        // current term
	votedFor    int        // id of the server that me vote for
	log         []LogEntry // log entries
	commitIndex int        // index of the highest log entry known to be committed
	lastApplied int        // index of the highest log entry applied to state machine
	nextIndex   []int      // index of the next log entry to that server (leader only)
	matchIndex  []int      // index of the highest log entry known to be replicated on server

	// extra fields for convenience
	leaderId           int           // leader's id
	isKilled           chan bool     // if is killed
	applyMsg           chan ApplyMsg // apply message when commit new log entry
	recvHeartbeatTimer *time.Timer   // timer of receiving heartbeat from leader
	sendHeartbeatTimer *time.Timer   // timer of sending heartbeat to others (leader only)
}

// resetRecvHeartbeatTimer
// reset timer of receiving heartbeat from leader
func (rf *Raft) resetRecvHeartbeatTimer() {
	timeOut := int64(recvHeartbeatTimeOutL + rand.Intn(recvHeartbeatTimeOutR-recvHeartbeatTimeOutL))
	rf.recvHeartbeatTimer.Reset(time.Duration(timeOut * int64(time.Millisecond)))
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.me == rf.leaderId
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

// readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data != nil {
		d := gob.NewDecoder(bytes.NewBuffer(data))
		_ = d.Decode(&rf.currentTerm)
		_ = d.Decode(&rf.votedFor)
		_ = d.Decode(&rf.log)
	} else {
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Term: -1, Command: nil}
	}
}

// RequestVoteArgs
// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply
// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // if candidate received vote
}

// RequestVote
// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// receiver:
	// rf.lock() & defer rf.unlock()
	// reply.term = rf.term
	// if args.term < rf.term
	// 	reply.grant = false

	// if args.term > rf.term
	// 	rf.term = args.term
	// 	if rf is leader
	// 		rf change to be follower
	//
	// if (rf.votedFor == nobody || rf.votedFor == args.candidateId)
	//  	&& candidate's log is at least as up-to-date as receiver's log
	// 	(`as up-to-date as' means:
	//	1. args.lastLogIndex > rf.lastLogIndex
	// 	2. args.lastLogIndex == rf.lastLogIndex && args.lastLogTerm >= rf.lastLogTerm
	//	)
	// 	rf.votedFor = args.candidateId
	// 	reply.grant = true

	//fmt.Printf("server[%d] receive RequestVote RPC: term=%d, candidateId=%d, lastLogIndex=%d, lastLogTerm=%d\n",
	//	rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.me == rf.leaderId {
			rf.leaderId = -1
		}
	}
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogIndex > lastLogIndex || (args.LastLogIndex == lastLogIndex && args.LastLogTerm >= lastLogTerm)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetRecvHeartbeatTimer()
	}

}

// broadcastRequestVote
// broadcast RequestVote RPC to peers, collect result and return if me win
func (rf *Raft) broadcastRequestVote() bool {
	// rf.mu.lock()
	// args := {term: rf.term, candidateId: rf.me, lastLogIndex: rf.lastLogIndex, lastLogTerm: rf.lastLogTerm}
	// voterNum = len(rf.peers)
	// voteResult = make(chan bool, voterNum)
	// foreach server[i] in peers:
	// 	goroutine:
	// 		reply := {}
	// 		if server[i].RPC("Raft.RequestVote", args, reply)
	// 			rf.lock()
	//			if reply.term > rf.term:
	//				rf.term = reply.term
	// 				rf.votedFor = -1
	// 			rf.unlock()
	// 		voteResult <- reply.VoteGranted

	// grantNum = 0
	// notGrantNum = 0
	// select from voteResult, count grantNum and notGrantNum
	// return granNum > notGrantNum

	rf.mu.Lock()
	rf.leaderId = -1
	rf.currentTerm += 1
	rf.votedFor = -1
	rf.resetRecvHeartbeatTimer()
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	rf.mu.Unlock()
	voterNum := len(rf.peers)
	voteResult := make(chan bool, voterNum)
	for i := 0; i < voterNum; i++ {
		go func(serverId int) {
			reply := &RequestVoteReply{VoteGranted: false}
			if rf.leaderId == -1 && args.Term == rf.currentTerm && rf.peers[serverId].Call("Raft.RequestVote", args, reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}
			voteResult <- reply.VoteGranted
		}(i)
	}

	grantNum := 0
	notGrantNum := 0
	for i := 0; i < voterNum; i++ {
		select {
		case grant := <-voteResult:
			if grant {
				grantNum += 1
			} else {
				notGrantNum += 1
			}
			if grantNum*2 > voterNum || notGrantNum*2 > voterNum || rf.leaderId != -1 {
				break
			}
		case <-time.After(RPCTimeOut): // voter may disconnect, so check timeout here
			break
		}
	}
	return grantNum*2 > voterNum // voter disconnect
}

// AppendEntriesArgs
// AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply
// result of AppendEntries RPC
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// applyCommits
// apply commits from [rf.lastApplied + 1 : applyEnd]
func (rf *Raft) applyCommits(applyEnd int) {
	// rf.mu.lock()
	// get applyMsg entry and notify rf.applyMsg
	// rf.mu.unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	go rf.persist()
	for rf.lastApplied < applyEnd {
		rf.lastApplied += 1
		var applyMsg ApplyMsg
		applyMsg.Index = rf.lastApplied
		applyMsg.Command = rf.log[applyMsg.Index].Command
		rf.applyMsg <- applyMsg
	}
}

// min
// return min(a, b)
func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

// AppendEntries
// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// receiver:
	// reset timer for receiving heartbeat
	// if args.term < rf.term || rf.log[args.PrevLogIndex].term != args.PrevLogTerm
	// 	reply.success = false
	//	return
	// reply.success = true
	// update rf.leaderId = args.leaderId
	// if args.term > rf.term
	// 	rf.term = args.term
	// 	rf.votedFor = -1
	// 	if rf is leader
	// 		rf change to follower
	// if AppendEntries is not heartbeat
	// 	append the args.Entries to logs
	// if an existing entry conflicts with a new one (same index, different term)
	// 	delete rf.log[conflict index:-1]
	// Append any new entries not already in rf.log[]
	// if args.LeaderCommit > rf.commitIndex
	// 	rf.commitIndex = min(args.LeaderCommit, index of last new entry)

	rf.resetRecvHeartbeatTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.leaderId == rf.me {
			rf.leaderId = -1
		}
	}
	rf.leaderId = args.LeaderId

	// remove conflict entries and append new entries
	if args.Entries != nil { // not heartbeat
		lastEntryIndex := 0
		for args.PrevLogIndex++; args.PrevLogIndex < len(rf.log) && lastEntryIndex < len(args.Entries); args.PrevLogIndex++ {
			rf.log[args.PrevLogIndex] = args.Entries[lastEntryIndex]
			lastEntryIndex++
		}
		for ; lastEntryIndex < len(args.Entries); lastEntryIndex++ {
			rf.log = append(rf.log, args.Entries[lastEntryIndex])
		}
	}
	// update commit index, then apply commit message
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyCommits(rf.commitIndex)
	}

}

// broadcastAppendEntries
// broadcast AppendEntries RPC
func (rf *Raft) broadcastAppendEntries(updateLogEnd int) {
	// sender:
	// reset timer for sending heartbeat
	// send AppendEntries to peers
	// if rpc fail, retry
	// if other response term > currentTerm, convert to follower
	// if success && not heartbeat
	// 	update next index and match index
	// else if heartbeat
	// 	the logEnd is -1
	// else if RPC fail
	// 	update the logEnd
	//
	// wait for result and decide to commit

	rf.sendHeartbeatTimer.Reset(heartbeatInterval)
	appenderNum := len(rf.peers)
	appendResult := make(chan bool, appenderNum)
	thisTerm := rf.currentTerm
	// send to every peers
	for i := 0; i < appenderNum; i++ {
		// accept self
		if i == rf.me {
			appendResult <- true
			rf.resetRecvHeartbeatTimer()
			continue
		}
		go func(serverId int, logEnd int) {
			reply := &AppendEntriesReply{Success: false}
			var sendEntries []LogEntry = nil
			for {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[serverId] - 1
				if logEnd != -1 { // not heart beat
					if rf.leaderId != rf.me || rf.nextIndex[serverId] >= logEnd { // if other goroutine has sent, just return
						rf.mu.Unlock()
						break
					}
					sendEntries = rf.log[rf.nextIndex[serverId]:logEnd]
				}
				args := AppendEntriesArgs{Entries: sendEntries,
					PrevLogIndex: prevLogIndex, PrevLogTerm: rf.log[prevLogIndex].Term,
					LeaderId: rf.me, Term: thisTerm, LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()

				retryTimes := networkFailRetryTimes
				retryWaitTime := time.Nanosecond
				for retryTimes >= 0 && !rf.peers[serverId].Call("Raft.AppendEntries", args, reply) {
					retryTimes -= 1
					retryWaitTime <<= 1
					time.Sleep(retryWaitTime)
				}

				if reply.Term > rf.currentTerm || rf.leaderId != rf.me {
					rf.mu.Lock()
					rf.leaderId = -1
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.resetRecvHeartbeatTimer()
					rf.mu.Unlock()
					break
				}
				if reply.Success {
					if logEnd != -1 { // if not heart beat
						rf.mu.Lock()
						if rf.nextIndex[serverId] < logEnd-1 {
							rf.nextIndex[serverId] = logEnd - 1
							rf.matchIndex[serverId] = logEnd - 1
						}
						rf.mu.Unlock()
					}
					break
				} else { // RPC fail or not match
					rf.mu.Lock()
					logEnd = len(rf.log)
					if rf.nextIndex[serverId] > 1 {
						rf.nextIndex[serverId] -= 1
					}
					rf.mu.Unlock()
				}
			}
			appendResult <- reply.Success
		}(i, updateLogEnd)
	}

	// wait for result
	replyOk := 0
	for i := 0; i < appenderNum && rf.leaderId == rf.me; i++ {
		select { // wait for send results
		case r := <-appendResult:
			if updateLogEnd == -1 && i > 1 { // heart beat
				return
			}
			if updateLogEnd != -1 && r { // not heart beat
				replyOk += 2
				// if the majority of servers accept, commit it
				if replyOk > appenderNum {
					newCommitIndex := updateLogEnd - 1
					rf.mu.Lock()
					if newCommitIndex > rf.commitIndex {
						rf.commitIndex = newCommitIndex
						go rf.applyCommits(newCommitIndex) // apply these commits
					}
					rf.mu.Unlock()
					return
				}
			}
		case <-time.After(RPCTimeOut):
			rf.mu.Lock()
			// if the leader is disconnect from network, it will only receive the response from leader self
			if i == 1 && rf.leaderId == rf.me {
				rf.leaderId = -1 // convert to follower
			}
			rf.mu.Unlock()
			return
		}
	}

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
	// rf.mu.lock()
	// if rf is leader
	// 	index = len(rf.log)
	// 	append rf.log with LogEntry{rf.term, command}
	// 	broadcast AppendEntries(len(rf.log))
	// else
	// 	index = -1
	// rf.mu.unlock()
	// return index, rf.term, isLeader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.leaderId == rf.me
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		go rf.broadcastAppendEntries(len(rf.log))
	}
	return index, term, isLeader

}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.isKilled <- true
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.leaderId = -1
	rf.isKilled = make(chan bool)
	rf.applyMsg = applyCh
	rf.recvHeartbeatTimer = time.NewTimer(0)
	rf.sendHeartbeatTimer = time.NewTimer(0)
	rf.resetRecvHeartbeatTimer()
	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// main goroutine:
	// 	for loop:
	// 		if rf is leader
	// 			case: rf.isKilled
	//				return
	// 			case: sendHeartbeatTimer time out
	// 				rf.sendAppendEntries(empty)
	// 		else
	// 			case: rf.isKilled
	// 				return
	// 			case: recvHeartbeatTimer time out
	// 				rf changed to candidate
	// 				start election
	// 				win = rf.broadcastRequestVote()
	// 				if win
	// 					rf.mu.lock()
	// 					rf.leaderId = rf.me
	// 					foreach server[i] in rf.peers:
	// 						rf.nextIndex[i] = rf.lastLogIndex
	// 						rf.matchIndex[i] = 0
	// 					rf.mu.unlock()
	// 					rf.sendAppendEntries(empty)

	go func() {
		for {
			if rf.me == rf.leaderId {
				select {
				case <-rf.isKilled:
					return
				case <-rf.sendHeartbeatTimer.C:
					go rf.broadcastAppendEntries(-1)
				}
			} else {
				select {
				case <-rf.isKilled:
					return
				case <-rf.recvHeartbeatTimer.C:
					//fmt.Printf("server[%d] start election\n", rf.me)
					win := rf.broadcastRequestVote()
					if win && rf.leaderId == -1 {
						rf.mu.Lock()
						rf.leaderId = rf.me
						//fmt.Printf("server[%d] become leader\n", rf.me)
						lastLogIndex := len(rf.log)
						for i := range rf.nextIndex { // dont use for i:=0; i < len(rf.nextIndex); i++, will cause error
							rf.nextIndex[i] = lastLogIndex
							rf.matchIndex[i] = 0
						}
						rf.mu.Unlock()
						go rf.broadcastAppendEntries(-1)
					}
				}
			}
		}
	}()

	return rf

}
