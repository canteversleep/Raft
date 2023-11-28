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

type LogEntry struct {
	Term    int
	Command interface{}
}

type MEMBER_STATE int

const (
	leader MEMBER_STATE = iota
	candidate
	follower
)

func (e MEMBER_STATE) String() string {
	switch e {
	case leader:
		return "leader"
	case candidate:
		return "candidate"
	case follower:
		return "follower"
	}
	return ""
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// leader volatile state
	nextIndex  []int
	matchIndex []int

	// states for orchestrating elections beyond whats on the paper.
	// we have channels for conveying news of election and thus triggering a stepdown,
	//  relaying heartbeats for resetting timers, and for conveying news of a within-server
	// vote to a candidate thread
	state           MEMBER_STATE
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

// other getters and setters here

func (rf *Raft) lastIndex() int {
	return len(rf.log) - 1
}

// non blocking send to chan
func (rf *Raft) relay(channel chan int, msg string) {
	select {
	case channel <- 1:
	default:
		// DPrintf("over %s as %d from %d\n", msg, rf.state, rf.me)
	}
}

func (rf *Raft) sanitizeChannels() {
	rf.electionNotice = make(chan int)
	rf.heartbeatNotice = make(chan int)
	rf.voteNotice = make(chan int)
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
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// TODO: important! this is likely where we have state mashup. we would like to grant votes but we may be a candidate or even a leader
	// the problem is that resetElectionState does not ensure the server becomes a follower prior to receiving the first heartbeat. since this
	// entire function is atomic. a refactor could be to switch state to follower within resetElectionState
	if args.Term > rf.currentTerm {
		rf.resetElectionState(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.relay(rf.voteNotice, "vote")
	}
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
	ok := rf.sendRequestVote(server, args, &response)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// catch some rare race conditions: particularly, if the response is coming after the state has changes from cand,
	// to either follower or leader, or when the request or response is for a previous election <- could happen
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.resetElectionState(args.Term)
	}

	reply.Term = rf.currentTerm

	rf.relay(rf.heartbeatNotice, "heart")
	// SYNC
}

func (rf *Raft) sendAppendEntriesWrapper(server int, args AppendEntriesArgs) {
	var response AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &response)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// again some race conditions regarding state transitions similar to the ones handled by sendRequestVoteWrapper
	if rf.state != leader || args.Term != rf.currentTerm || response.Term < rf.currentTerm {
		return
	}

	if response.Term > rf.currentTerm {
		rf.resetElectionState(response.Term)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, rf.currentTerm, false
	}

	// NOTE: may want to load term into own var. but mutex should allow repeated access from rf object
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})

	return rf.lastIndex(), rf.currentTerm, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetElectionState(newTerm int) {
	rf.votedFor = -1
	rf.currentTerm = newTerm
	if rf.state != follower {
		rf.relay(rf.electionNotice, "election")
		// rf.switchState(follower)
	}
}

func (rf *Raft) switchState(newState MEMBER_STATE) {
	rf.mu.Lock()
	// whenever we switch state, we might have some stale requests arriving belonging to other shit. for this purpose we
	// reset the channels
	rf.sanitizeChannels()
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

// follower function. fairly straightforward
func (rf *Raft) dispatchFollower() {
	for {
		select {
		case <-rf.heartbeatNotice:
		case <-rf.voteNotice:
		case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
			rf.switchState(candidate)
			return
		}
	}
}

// candidate state function. handles setting up an election and then promoting to leader or resetting
func (rf *Raft) dispatchCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	me := rf.me
	localTerm := rf.currentTerm
	rf.mu.Unlock()
	// we create a channel to tally the votes
	tally := make(chan int)
	totalVotes := 1
	// We probably want to broadcast votes as a goroutine in order to be ready to tally votes as soon as possible.
	// OTOH, dispatching this as a goroutine means the term starts prior to all the requestvotes being sent out,
	// possible, since there is a mutex that needs to be acquired. This means a reelection might be triggered
	// more often than what is necessary
	// From a point of view of atomicity, we might want this to proceed with a goroutine, since the spec does not
	// specify any ordering for the candidate operations.
	go func() {
		requestVoteArg := RequestVoteArgs{
			Term:        localTerm,
			CandidateId: me,
		}
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendRequestVoteWrapper(i, requestVoteArg, tally)
			}
		}
	}()

	for {
		select {
		case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
			rf.switchState(candidate)
			return
		case <-tally:
			totalVotes++
			if totalVotes > len(rf.peers)/2 {
				rf.switchState(leader)
				return
			}
		case <-rf.electionNotice:
			rf.switchState(follower)
			return
		}
	}
}

// leader function. sends appendentries periodically
func (rf *Raft) dispatchLeader() {
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.lastIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex
	}
	rf.mu.Unlock()

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			// send heart
			rf.mu.Lock()
			for i := range rf.peers {
				if i != rf.me {
					args := AppendEntriesArgs{
						Term: rf.currentTerm,
					}
					go rf.sendAppendEntriesWrapper(i, args)
				}
			}
			rf.mu.Unlock()
		case <-rf.electionNotice:
			rf.switchState(follower)
			return
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

	rf.mu.Lock()
	rf.votedFor = -1
	rf.state = follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionNotice = make(chan int)
	rf.heartbeatNotice = make(chan int)
	rf.voteNotice = make(chan int)
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) // does nothing if first init

	rf.switchState(rf.state)

	return rf
}
