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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type MEMBER_STATE int

const (
	leader MEMBER_STATE = iota
	candidate
	follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain
	// TODO: add remainder state for part 2 and possibly remove isLeader
	currentTerm int
	votedFor    int
	state       MEMBER_STATE

	// DEPRECATED
	// timer and channel in case i am a follower
	// timer           *time.Timer
	// electionTimeout time.Duration
	// put channel for immediate demotion of leaders/candidates in case of special election
	// this channel's use is restricted to receipt of RequestVote and AmendLog RPCs when state is leader or candidate
	// a handler is dispatched which then reverts leaders and candidates to followers
	electionNotice  chan int
	heartbeatNotice chan int
	voteNotice      chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == leader
	// Your code here.
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.resetElectionState(args.Term)
		rf.mu.Lock()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.mu.Unlock()
		rf.voteNotice <- 1
		rf.mu.Lock()
	}
	defer rf.mu.Unlock()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVoteWrapper(server int, args RequestVoteArgs, replyTo chan int) {
	var response RequestVoteReply
	if !rf.sendRequestVote(server, args, &response) {
		return
	}

	// catch some rare race conditions
	if rf.state != candidate || args.Term != rf.currentTerm || response.Term < rf.currentTerm {
		return
	}

	if response.Term > rf.currentTerm {
		rf.resetElectionState(response.Term)
		return
	} else if response.VoteGranted {
		replyTo <- 1
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int
}

// example RequestVote RPC reply structure.
type AppendEntriesReply struct {
	Term int
	// Success bol
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// HEARTBEAT PORTION
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.resetElectionState(args.Term)
	}

	reply.Term = rf.currentTerm

	if rf.state == follower {
		rf.heartbeatNotice <- 1
	}
	// SYNC
}

// TODOedit later to delegate functionality to leader dispatch
func (rf *Raft) sendAppendEntriesWrapper(server int) {
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
	}
	var response AppendEntriesReply
	if rf.sendAppendEntries(server, args, &response) {
		if response.Term > rf.currentTerm {
			rf.resetElectionState(response.Term)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetElectionState(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = -1
	rf.currentTerm = newTerm
	if rf.state == leader || rf.state == candidate {
		rf.electionNotice <- 1
	}
}

func (rf *Raft) switchState(newState MEMBER_STATE) {
	rf.mu.Lock()
	rf.state = newState
	rf.mu.Unlock()
	switch newState {
	case follower:
		go rf.dispatchFollower()
	case candidate:
		go rf.dispatchCandidate()
	case leader:
		go rf.dispatchLeader()
	}
}

func (rf *Raft) dispatchFollower() {
	for rf.state == follower {
		select {
		case <-rf.heartbeatNotice:
		case <-rf.voteNotice:
		case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
			rf.switchState(candidate)
		}
	}
}

func (rf *Raft) dispatchCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	localTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.mu.Unlock()
	// we create a channel to tally the votes
	tally := make(chan int)
	totalVotes := 1
	go func() {
		requestVoteArg := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendRequestVoteWrapper(i, requestVoteArg, tally)
			}
		}
	}()

	for rf.state == candidate && rf.currentTerm == localTerm {
		select {
		case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
			rf.switchState(candidate)
		case <-tally:
			totalVotes++
			if totalVotes > len(rf.peers)/2 {
				rf.switchState(leader)
			}
		case <-rf.electionNotice:
			rf.switchState(follower)
		}
	}
}

func (rf *Raft) dispatchLeader() {

	for rf.state == leader {
		select {
		case <-time.After(100 * time.Millisecond):
			// send heart
			for i := range rf.peers {
				if i != rf.me {
					go func(server int) {
						rf.sendAppendEntriesWrapper(server)
					}(i)
				}
			}
		case <-rf.electionNotice:
			rf.switchState(follower)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	rf.mu.Lock()
	rf.votedFor = -1
	rf.state = follower
	rf.currentTerm = 0
	rf.electionNotice = make(chan int)
	rf.heartbeatNotice = make(chan int)
	rf.voteNotice = make(chan int)
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) // does nothing if first init

	rf.switchState(rf.state)

	return rf
}
