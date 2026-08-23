package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func annotate(id int, desc, details string) {
	server := fmt.Sprintf("Server %v", id)
	tester.Annotate(server, desc, details)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	prevCommitIndex int
	currentTerm     int
	status          NodeState
	votedFor        int
	rpcReceived     bool
	// []Log     have to check what it looks like

	// Temporary
	LASTLOGINDEXTMP int
	LASTLOGTERMTMP  int
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{} // Needs to be
	LeaderCommit int           // Leader's commit index
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	annotate(rf.me, fmt.Sprintf("HB from %v", args.LeaderId), "")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	rf.rpcReceived = true
	if rf.status != Leader || args.Term > rf.currentTerm {
		rf.status = Follower
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return rf.currentTerm, rf.status == Leader
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
	Term         int
	CandidateId  int
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
	DPrintf("%v receieved RequestVote from %v", rf.me, args.CandidateId)
	rf.mu.Lock()
	DPrintf("%v receieved RequestVote from %v. Mutex locked", rf.me, args.CandidateId)
	defer rf.mu.Unlock()
	desc := ""
	defer func() {
		annotate(rf.me, fmt.Sprintf("Vote? %v. From %v", reply.VoteGranted, args.CandidateId), desc)
	}()

	// Your code here (3A, 3B).
	if args.Term < rf.currentTerm {
		desc = "Lower term"
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.rpcReceived = true

	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.status = Follower
	}
	if args.Term == rf.currentTerm && rf.votedFor != args.CandidateId {
		desc = "Already voted this term"
		reply.VoteGranted = false
		return
	}
	rf.currentTerm = args.Term
	desc = "Voted"
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
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
	DPrintf("%v sendRequestVote %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("%v sendRequestVote %v received", rf.me, server)
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

func (rf *Raft) startElection() {
	DPrintf("%v starting election", rf.me)
	defer func() {
		DPrintf("%v exiting startElection", rf.me)
	}()
	rf.mu.Lock()
	rf.status = Candidate
	rf.currentTerm += 1
	voters := []int{}
	for voterId := range rf.peers {
		if voterId != rf.me {
			voters = append(voters, voterId)
		}
	}
	votes := 1
	votesNeeded := int(math.Floor(float64(len(rf.peers))/2.)) + 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		LastLogIndex: rf.LASTLOGINDEXTMP,
		LastLogTerm:  rf.LASTLOGTERMTMP,
		CandidateId:  rf.me,
	}
	rf.mu.Unlock()

	// Timout
	timeout := time.After(300 * time.Millisecond)
	for rf.status == Candidate && len(voters) > 0 && votes < votesNeeded {
		rf.mu.Lock()
		DPrintf("%v looping. votes: %v, needvotes: %v", rf.me, votes, votesNeeded)
		rf.mu.Unlock()
		for len(voters) > 0 && votes < votesNeeded {
			rf.mu.Lock() //2
			DPrintf("%v looping\n", rf.me)
			rf.mu.Unlock() //2
			select {
			case _ = <-timeout:
				rf.status = Follower
				return
			default:
				rf.mu.Lock() //3

				DPrintf("%v startElection hasn't timed out", rf.me)
				rf.mu.Unlock() //4
			}
			voterId := voters[rand.Intn(len(voters))]

			reply := &RequestVoteReply{}
			rf.mu.Lock() //5
			annotate(rf.me, fmt.Sprintf("RequestVote %v", voterId), "")
			DPrintf("%v RequestVote -> %v", rf.me, voterId)
			rf.mu.Unlock() //5
			if !rf.sendRequestVote(voterId, args, reply) {
				DPrintf("%v RequestVote -> %v failed", rf.me, voterId)
				annotate(rf.me, fmt.Sprintf("RequestVote %v: failed", voterId), "")
				continue
			}
			annotate(rf.me, fmt.Sprintf("RequestVote %v, %v", voterId, reply.VoteGranted), "")
			voters = slices.DeleteFunc(voters, func(v int) bool { return v == voterId })
			rf.mu.Lock() //7

			DPrintf("%v RequestVote -> %v got response", rf.me, voterId)
			DPrintf("%v hasnt timed out", rf.me)

			// Current node is behind, quit being a Candidate

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = Follower
				rf.mu.Unlock() //8
				return
			}

			if reply.VoteGranted {
				votes += 1
			}
			rf.mu.Unlock()

		}
	}

	DPrintf("%v Suff votes: %v, isCandidate: %v", rf.me, votes >= votesNeeded, rf.status == Candidate)
	if votes >= votesNeeded && rf.status == Candidate {
		DPrintf("%v has become leader", rf.me)
		rf.status = Leader
	}
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a Leader election should be started.
		rf.mu.Lock()
		if rf.status != Leader && !rf.rpcReceived {
			DPrintf("%v starting election", rf.me)
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
			DPrintf("ID: %v, status: %v, rpcReceieved: %v. Did not start election\n", rf.me, rf.status, rf.rpcReceived)
		}

		// Pause while leader
		rf.mu.Lock()
		for rf.status == Leader {
			rf.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		rf.mu.Lock()
		rf.rpcReceived = false
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeats() {
	for {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			continue
		}
		args := &AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.LASTLOGINDEXTMP,
			PrevLogTerm:  rf.LASTLOGTERMTMP,
			Entries:      []interface{}{},
			LeaderCommit: rf.prevCommitIndex,
		}
		DPrintf("%v sending heartbeats", rf.me)
		reply := &AppendEntryReply{}
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			// peer called. caller. response.
			ok := peer.Call("Raft.AppendEntry", args, reply)
			annotate(rf.me, fmt.Sprintf("Heartbeat %v. Status: %v", i, ok), "")
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = Follower
				break
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) updates() {
	for {
		// i want to know the status and the
		rf.mu.Lock()
		DPrintf("%v status: %v", rf.me, rf.status)
		desc := fmt.Sprintf("Status: %v\n", rf.status)
		annotate(rf.me, "Status", desc)
		rf.mu.Unlock()
		time.Sleep(1 * time.Second)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.prevCommitIndex = -1
	rf.currentTerm = 0
	rf.status = Follower
	rf.votedFor = -1
	rf.rpcReceived = true

	// Temporary
	rf.LASTLOGINDEXTMP = 4
	rf.LASTLOGTERMTMP = 4

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeats()
	go rf.updates()
	return rf
}
