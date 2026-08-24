package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
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

type EntryLog struct {
	term  int
	entry interface{}
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
	logs            []EntryLog //have to check what it looks like

	matchIndex []int
	nextIndex  []int
	// Temporary
	LASTLOGINDEXTMP int
	LASTLOGTERMTMP  int
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []EntryLog // Needs to be
	LeaderCommit int        // Leader's commit index
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	annotate(rf.me, fmt.Sprintf("HB from %v", args.LeaderId), "")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.rpcReceived = true
	if rf.status != Leader || args.Term > rf.currentTerm {
		rf.status = Follower
	}
	rf.currentTerm = args.Term
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return -1, -1, false
	}
	rf.logs = append(rf.logs, EntryLog{
		term:  rf.currentTerm,
		entry: command,
	},
	)

	// command is the entry that needs to be committed
	// the current server needs to be the leader to follow
	// through. if it is not the leader it returns falseit will be 0 indexed.
	// the term is just the current term of the server. the index is the
	// existing last index of the log array+1 i.e. equal to the length
	// of the log array.

	// Your code here (3B).

	return len(rf.logs) - 1, rf.currentTerm, true
}

func (rf *Raft) startElection() {
	annotate(rf.me, "starting election", "")
	rf.mu.Lock()

	rf.status = Candidate
	rf.currentTerm += 1
	votes := 1
	votesNeeded := int(math.Floor(float64(len(rf.peers))/2.)) + 1

	// Timout
	timeout := time.After(300 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(term int, ctx context.Context) {
			for {
				rf.mu.Lock()
				if votes >= votesNeeded || rf.status != Candidate {
					rf.mu.Unlock()
					return
				}
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: rf.LASTLOGINDEXTMP,
					LastLogTerm:  rf.LASTLOGTERMTMP,
				}
				reply := &RequestVoteReply{}
				rf.mu.Unlock()
				annotate(rf.me, fmt.Sprintf("RequestVote -> %v", i), "")
				if !peer.Call("Raft.RequestVote", args, reply) {
					continue
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = Follower
				} else if reply.VoteGranted {
					votes += 1
				}
				rf.mu.Unlock()
				return
			}
		}(rf.currentTerm, ctx)
	}
	rf.mu.Unlock()
Loop:
	for {
		select {
		case _ = <-timeout:
			rf.mu.Lock()
			rf.status = Follower
			rf.mu.Unlock()
			break Loop
		default:
			DPrintf("%v startElection hasn't timed out", rf.me)
		}

		rf.mu.Lock()
		if rf.status == Follower {
			annotate(rf.me, "Become follower", "")
			rf.mu.Unlock()
			break Loop
		}
		if votes >= votesNeeded {
			annotate(rf.me, "Leader", "")
			rf.status = Leader
			rf.mu.Unlock()
			break Loop
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
	cancel()
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
	rf.mu.Lock()
	wasLeader := rf.status == Leader
	matchIndex := make([]int, len(rf.peers))
	nextIndex := make([]int, len(rf.peers))
	for i := range nextIndex {
		nextIndex[i] = len(rf.logs) - 1
	}
	heartbeat := func(index int) {
		if index == rf.me {
			return
		}

		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			return
		}
		args := &AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.LASTLOGINDEXTMP,
			PrevLogTerm:  rf.LASTLOGTERMTMP,
			Logs:         []EntryLog{},
			LeaderCommit: rf.prevCommitIndex,
		}
		for rf.peers[index].Call("Raft.AppendEntry", args, reply) {
			rf.mu.Lock()
			time.Sleep(1 * time.Millisecond) // so
			// stop since we are not a leader anymore
			if rf.status != Leader {
				return
			}

			// if we failed to send the rpc lets try again
			if !appendentry {
				continue
			}

			//appendentry failed so lets check if it was
			nextIndex[index] -= 1
			args.Logs = rf.logs[nextIndex[index]:]
		}
		if reply.Term > rf.currentTerm && reply.Success {
			log.Fatalf("reply.Term > rf.currentTerm && reply.Success")
		}
		if matchIndex[index] != len(rf.logs)-1 {
			args.Logs = rf.logs[nextIndex[index]:]
		}

		reply := &AppendEntryReply{}
		rf.mu.Unlock()
		ok := rf.peers[index].Call("Raft.AppendEntry", args, reply)
		rf.mu.Lock()
		annotate(rf.me, fmt.Sprintf("Heartbeated %v. Status: %v, term: %v", index, ok, reply.Term), "")
		if reply.Term > rf.currentTerm && reply.Success {
			log.Fatalf("reply.Term > rf.currentTerm && reply.Success")
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.mu.Unlock()
			return
		} else if reply.Success {

		}
		if rf.status == Leader {

		}
		rf.mu.Unlock()
	}
	rf.mu.Unlock()

	for {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()

		// Reset volatile index state. needs to be recalculated
		// next time the node becomes leader
		if wasLeader && rf.status != Leader {
			for i := range matchIndex {
				matchIndex[i] = 0
				nextIndex[i] = 0
			}
		}
		if rf.status != Leader {
			if wasLeader {
				wasLeader = false

			}
			rf.mu.Unlock()
			continue
		}
		for i := range nextIndex {
			nextIndex[i] = len(rf.logs) - 1
		}
		wasLeader = true
		rf.mu.Unlock()
		for i := range rf.peers {
			// peer called. caller. response.
			annotate(rf.me, fmt.Sprintf("Heartbeating %v", i), "")
			go heartbeat(i)
		}
	}
}

func (rf *Raft) updates() {
	for {
		// i want to know the status and the
		rf.mu.Lock()
		DPrintf("%v status: %v, term: %v", rf.me, rf.status, rf.currentTerm)
		desc := fmt.Sprintf("Status: %v, Term: %v\n", rf.status, rf.currentTerm)
		annotate(rf.me, "Status", desc)
		rf.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (rf *Raft) syncer(index int) {
	if index == rf.me {
		return
	}
	rf.mu.Lock()
	wasntLeader := rf.status == Leader
	rf.mu.Unlock()
	rf.mu.Lock()
	if rf.status != Leader {
		rf.mu.Unlock()
		return
	}
	for {
		rf.mu.Lock()
		// Wait until becoming leader
		if rf.status != Leader {
			wasntLeader = true
			time.Sleep(1 * time.Millisecond)
			rf.mu.Unlock()
			continue
		}

		// Reset match index if just became a leader
		if wasntLeader {
			wasntLeader = false
			rf.matchIndex[index] = 0
			rf.nextIndex[index] = len(rf.logs) - 1
		}
		rf.mu.Unlock()

		// Keep waiting until there is a new log to append
		for {
			rf.mu.Lock()
			status := rf.status
			matchedUntil := rf.matchIndex[index]
			lenLogs := len(rf.logs)
			rf.mu.Unlock()
			if status != Leader || matchedUntil < lenLogs {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}

		// Check if still leader
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		// No longer leader so quit
		if status != Leader {
			continue
		}

		args := &AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[index] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[index]-1].term,
			Logs:         rf.logs[rf.nextIndex[index]:],
			LeaderCommit: rf.prevCommitIndex,
		}
		reply := &AppendEntryArgs{}
		rf.mu.Unlock()

		for rf.peers[index].Call("Raft.AppendEntry", args, reply) {
			rf.mu.Lock()
			time.Sleep(1 * time.Millisecond) // so
			// stop since we are not a leader anymore
			if rf.status != Leader {
				return
			}

			// if we failed to send the rpc lets try again
			if !appendentry {
				continue
			}

			//appendentry failed so lets check if it was
			nextIndex[index] -= 1
			args.Logs = rf.logs[nextIndex[index]:]
		}
		if reply.Term > rf.currentTerm && reply.Success {
			log.Fatalf("reply.Term > rf.currentTerm && reply.Success")
		}
		if matchIndex[index] != len(rf.logs)-1 {
			args.Logs = rf.logs[nextIndex[index]:]
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		prevCommitIndex: 0,
		currentTerm:     0,
		status:          Follower,
		votedFor:        -1,
		rpcReceived:     true,

		matchIndex: make([]int, len(peers)),
		nextIndex:  make([]int, len(peers)),

		LASTLOGINDEXTMP: 4,
		LASTLOGTERMTMP:  4,
	}
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
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.syncer(index)
	}
	return rf
}

/*
The leader logs something to itself and continuously
sends appendEntry rpcs to the other nodes for the logs that
they have not yet logged. once a majority has logged upto a
certain index the leads commits the index. on each appendEntry
rpc it tells them the latest commit index it is at.
the followers which receive the rpc commit up to that
index too (if they have logs up to that index).

leader gets a command
logs command to itself

§5.3 Log matching: If two logs contain an entry with
the same log index and term then the logs are identical up to
that point.

Ergo from 5.3, the current leader has to find the latest
entry with matching index and term for each peer and
send updates from that point forward. The leader's log
is canon at any point and is to be replicated. Leaders
never overwrite their own log but the followers do
overwrite their log if the leader tells them to given
that the leader's provided prevLogIndex and prevLogTerm match
i.e. the entries are same up to that point otherwise they need
to go further back.
thing to handle:
1. leader sends
*/
