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
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

func annotate(id int, desc, details string) {
	server := fmt.Sprintf("Server %v", id)
	tester.Annotate(server, desc, details)
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type EntryLog struct {
	Index int
	Term  int
	Entry interface{}
}

func (entry *EntryLog) String() string {
	return fmt.Sprintf("index: %v, term: %v, entry: %v", entry.Index, entry.Term, entry.Entry)
}

type SnapshotInfo struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan raftapi.ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	commitIndex int
	currentTerm int
	status      NodeState
	votedFor    int
	rpcReceived bool       // For election timeouts
	logs        []EntryLog //have to check what it looks like

	// Leader's volatile state
	matchIndex []int
	nextIndex  []int

	// Snapshot
	snapshotInfo SnapshotInfo
}

type AppendEntryArgs struct {
	Term               int
	LeaderId           int
	PrevLogChronoIndex int
	PrevLogTerm        int
	Logs               []EntryLog // Needs to be
	LeaderCommit       int        // Leader's commit index
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLogLen int
}

func (rf *Raft) termIsGreater(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return term > rf.currentTerm
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == Leader
}

func (rf *Raft) index(chronoIndex int) int {
	return chronoIndex - (rf.snapshotInfo.LastIncludedIndex + 1)
}
func (rf *Raft) indexLock(chronoIndex int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.index(chronoIndex)
}

func (rf *Raft) getLastIndexAndTerm() (int, int) {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
	}
	// No logs so the snapshot contains the latest log's index and term
	return rf.snapshotInfo.LastIncludedIndex, rf.snapshotInfo.LastIncludedTerm
}

func (rf *Raft) getLogLen() int {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Index + 1
	} else {
		return rf.snapshotInfo.LastIncludedIndex + 1
	}
}

func (rf *Raft) getLogLenLock() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLogLen()
}

func (rf *Raft) getLastIndexAndTermLock() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLastIndexAndTerm()
}

func (rf *Raft) stepDown(newTerm int) {
	rf.mu.Lock()
	defer rf.persistLock(nil)
	defer rf.mu.Unlock()
	annotate(rf.me, "Stepping down", fmt.Sprintf("Old term: %v, new term: %v", rf.currentTerm, newTerm))
	if rf.currentTerm > newTerm {
		log.Panicf("Asked to step down into a lower term")
	}
	rf.status = Follower
	if rf.currentTerm < newTerm {
		rf.votedFor = -1
		rf.currentTerm = newTerm
	}

	for i := range rf.matchIndex {
		if i == rf.me {
			rf.matchIndex[i] = len(rf.logs) - 1
		} else {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = max(1, len(rf.logs)-1)
		}
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.persistLock(nil)
	defer rf.mu.Unlock()
	if len(args.Logs) > 0 {
		original := slices.Clone(rf.logs)
		annotate(rf.me, fmt.Sprintf("AppendEntry from %v", args.LeaderId), fmt.Sprintf("Received term: %v. My term: %v", args.Term, rf.currentTerm))
		defer func() {
			annotate(rf.me, "AppendEntry permutations", fmt.Sprintf("From: %v\nAdded: %v\nFinal: %v", original, args.Logs, rf.logs))
		}()
	} else {
		annotate(rf.me, fmt.Sprintf("HB from %v", args.LeaderId), fmt.Sprintf("Received term: %v. My term: %v", args.Term, rf.currentTerm))
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.rpcReceived = true
	if rf.status == Candidate || args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.stepDown(args.Term)
		rf.mu.Lock()
	}
	if args.LeaderCommit > len(rf.logs) {
		log.Panicf("args.LeaderCommit > len(rf.logs)")
	}
	rf.commitIndex = max(args.LeaderCommit, rf.commitIndex)

	// Nothing to append, its a heartbeat message
	if len(args.Logs) == 0 {
		reply.Success = true
		return
	}

	if args.PrevLogChronoIndex < rf.snapshotInfo.LastIncludedIndex {
		log.Panicf("Leader tried to append at log index which has already been snapshotted")
	}

	// prevLogChrono index is greater than log
	if args.PrevLogChronoIndex >= rf.getLogLen() {
		reply.Success = false
		reply.XLogLen = rf.getLogLen() //essentially our new log length
		annotate(rf.me, "AppendEntry Rejected", "")
		annotate(rf.me, "PrevLogIndex too large", fmt.Sprintf("My log length: %v. Given PrevLogIndex: %v", rf.getLogLen(), args.PrevLogChronoIndex))
		reply.XLogLen = rf.getLogLen()
		annotate(rf.me, fmt.Sprintf("XLogLen: %v, XTerm: %v, XIndex: %v", reply.XLogLen, reply.XTerm, reply.XIndex), fmt.Sprintf("PrevLogIndex: %v, PrevLogTerm: %v, Mine: %v	\nReceived: %v", args.PrevLogChronoIndex, args.PrevLogTerm, rf.logs, args.Logs))
		return
	}

	var prevLogChronoIndex int = args.PrevLogChronoIndex
	var prevLogTerm int
	if args.PrevLogChronoIndex == rf.snapshotInfo.LastIncludedIndex {
		prevLogTerm = rf.snapshotInfo.LastIncludedTerm
	} else {
		prevLogTerm = rf.logs[rf.index(prevLogChronoIndex)].Term
	}
	// Term of previous log entry doesn't match
	if args.PrevLogTerm != prevLogTerm {
		reply.Success = false
		reply.XLogLen = -1
		reply.XTerm = prevLogTerm
		i := prevLogChronoIndex
		for ; rf.index(i) >= 0 && rf.logs[rf.index(i)].Term == reply.XTerm; i -= 1 {
		}
		reply.XIndex = i + 1
		annotate(rf.me, "PrevLogTerm doesn't match", fmt.Sprintf("My term (index %v): %v,given term: %v", args.PrevLogChronoIndex, rf.logs[args.PrevLogChronoIndex].Term, args.PrevLogTerm))
		return
	}

	if args.Logs[0].Index < rf.commitIndex {
		log.Panicf("Appendentry request below commit index")
	}
	rf.logs = slices.Concat(rf.logs[:rf.index(prevLogChronoIndex+1)], args.Logs)
	for i, entry := range rf.logs {
		if rf.index(entry.Index) != i {
			log.Panicf("Index does not match")
		}
	}
	reply.Success = true
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
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotInfo)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

func (rf *Raft) persistLock(snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist(snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(raftState []byte, snapshot []byte) {
	if raftState == nil || len(raftState) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(raftState)
	d := labgob.NewDecoder(r)
	var logs []EntryLog
	var currentTerm int
	var votedFor int
	var snapshotInfo SnapshotInfo
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotInfo) != nil {
		log.Panicf("Failed to decode")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotInfo = snapshotInfo
	}

	msgs := []raftapi.ApplyMsg{}
	if snapshot == nil {
		return
	}
	annotate(rf.me, "Received non-nil snapshot", "")
	if d.Decode(snapshot) != nil {
		annotate(rf.me, "Decoded snapshot into []rafapi.ApplyMsg{}", "")
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	} else {
		annotate(rf.me, "Failed to decode snapshot into []raftapi.ApplyMsg{}", "")
	}
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
func (rf *Raft) Snapshot(chronoIndex int, data []byte) { //data was originally named snapshot
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if chronoIndex >= len(rf.logs) {
		log.Panicf("Snapshot index >= len(rf.logs)")
	}
	if chronoIndex > rf.commitIndex {
		log.Panicf("Snapshot index > rf.commitIndex")
	}
	// Accidental snapshot when there is no new info to snapshot
	if len(rf.logs) == 0 {
		return
	}

	// Chronological index to array index
	arrayIndex := rf.index(chronoIndex)
	rf.snapshotInfo = SnapshotInfo{
		LastIncludedIndex: chronoIndex,
		LastIncludedTerm:  rf.logs[arrayIndex].Term,
	}
	rf.logs = slices.Clone(rf.logs[arrayIndex+1:])
	rf.persist(data)
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%v received RequestVote from %v", rf.me, args.CandidateId)
	rf.mu.Lock()
	DPrintf("%v received RequestVote from %v. Mutex locked", rf.me, args.CandidateId)
	desc := ""
	defer func() {
		annotate(rf.me, fmt.Sprintf("Vote? %v. From %v", reply.VoteGranted, args.CandidateId), desc)
	}()

	// Your code here (3A, 3B).
	// Check if their term is at least the same as ours
	if args.Term < rf.currentTerm {
		desc = "args.Term < rf.currentTerm"
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	rf.rpcReceived = true

	reply.Term = args.Term
	rf.mu.Unlock()
	if rf.termIsGreater(args.Term) {
		rf.stepDown(args.Term)
	}
	rf.mu.Lock()
	defer rf.persistLock(nil)
	defer rf.mu.Unlock()
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		desc = "Already voted this term"
		reply.VoteGranted = false
		return
	}
	// Check if their log is at least as up to date as ours
	myLastLogIndex, myLastLogTerm := rf.getLastIndexAndTerm()
	if args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
		desc = "Voted"
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		desc = "Log is behind"
		reply.VoteGranted = false
	}

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
	defer rf.persistLock(nil)
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return -1, -1, false
	}
	rf.logs = append(rf.logs, EntryLog{
		Index: rf.getLogLen(),
		Term:  rf.currentTerm,
		Entry: command,
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
	rf.votedFor = rf.me
	votes := 1
	votesNeeded := int(math.Floor(float64(len(rf.peers))/2.)) + 1

	rf.mu.Unlock()
	rf.persistLock(nil)
	rf.mu.Lock()
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
				myLastLogIndex, myLastLogTerm := rf.getLastIndexAndTerm()
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: myLastLogIndex,
					LastLogTerm:  myLastLogTerm,
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
				if rf.termIsGreater(reply.Term) {
					rf.stepDown(reply.Term)
				} else if reply.VoteGranted {
					rf.mu.Lock()
					votes += 1
					rf.mu.Unlock()
				}
				return
			}
		}(rf.currentTerm, ctx)
	}
	rf.mu.Unlock()
Loop:
	for {
		select {
		case _ = <-timeout:
			rf.stepDown(rf.currentTerm)
			break Loop
		default:
			// DPrintf("%v startElection hasn't timed out", rf.me)
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
	rf.persistLock(nil)
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
			DPrintf("ID: %v, status: %v, rpcReceieved: %v. Did not start election\n", rf.me, rf.status, rf.rpcReceived)
			rf.mu.Unlock()
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
			Term:               rf.currentTerm,
			LeaderId:           rf.me,
			PrevLogChronoIndex: 0,
			PrevLogTerm:        0,
			Logs:               []EntryLog{},
			LeaderCommit:       min(rf.matchIndex[index], rf.commitIndex),
		}
		reply := &AppendEntryReply{}
		rf.mu.Unlock()
		ok := rf.peers[index].Call("Raft.AppendEntry", args, reply)
		rf.mu.Lock()
		annotate(rf.me, fmt.Sprintf("Heartbeated %v. Status: %v, term: %v", index, ok, reply.Term), "")
		if reply.Term > rf.currentTerm && reply.Success {
			log.Fatalf("reply.Term > rf.currentTerm && reply.Success")
		}
		rf.mu.Unlock()
		if rf.termIsGreater(reply.Term) {
			rf.stepDown(reply.Term)
		}
	}

	for {
		time.Sleep(100 * time.Millisecond)
		if !rf.isLeader() {
			continue
		}
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

type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	SnapshotInfo SnapshotInfo
	Snapshot     []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Going to get deleted probably
// func (rf *Raft) getSnapshot() *SnapshotInfo {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	var snapshotInfo SnapshotInfo
// 	r := bytes.Buffer(rf.persister.ReadSnapshot())
// 	d := labgob.NewDecoder(r)
// 	if err := d.Decode(&snapshot); err != nil {
// 		log.Panicf("getSnapshot decode failed: %v", err)
// 	}
// 	return snapshot
// }

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.SnapshotInfo.LastIncludedIndex < rf.snapshotInfo.LastIncludedIndex {
		log.Panicf("args.SnapshotInfo.LastIncludedIndex < rf.snapshotInfo.LastIncludedIndex")
	}
	r := bytes.NewBuffer(args.Snapshot)
	d := labgob.NewDecoder(r)
	msgs := []raftapi.ApplyMsg{}
	if err := d.Decode(msgs); err != nil {
		log.Panicf("Failed to decode args.data into []ApplyMsg{}: %v", err)
	}
	for _, msg := range msgs {
		rf.applyCh <- msg
	}
	rf.Snapshot(args.SnapshotInfo.LastIncludedIndex, args.Snapshot)
}

/*
A lot of index related changes. For one, the EntryLog.Index no longer
corresponds to the index of the entry in the rf.logs array since rf.logs
gets truncated whenever a new snapshot is created. It does still correspond
with the order that the entry was added to the log. You can index into
rf.logs using rf.logs[index - rf.logs[0].index]. Ensure that you assert
len(rf.logs) > 0 before running above code. Code which relies on
EntryLog.Index being the same as its index in rf.logs needs to be updated.
I can find the code by searching instances of "rf.logs[", index,
lastlogindex, etc. For the syncer goroutine in the leader, it checks
if the nextindex it is trying to send to the follower is < Index of the
first log in rf.logs. If it is less then that means the log no longer
exists as is and has been snapshotted instead so it should call InstallSnapshot
instead of something else. The follower never has to dig into its snapshotted i.e.
already truncated logs to find a matching lastlogindex for the leader because
only committed logs get snapshotted.


Possible cases where we have to InstallSnapshot:


It is guaranteed
that the highest log index truncated is <= commitIndex because only
the logs which have been committed to the state get snapshotted.
The last log index in the snapshot is rf.logs[0].Index-1

*/

func (rf *Raft) syncer(index int) {
	if index == rf.me {
		return
	}
	wasLeader := rf.isLeader()
MainLoop:
	for {
		// Wait until becoming leader
		if !rf.isLeader() {
			wasLeader = false
			time.Sleep(1 * time.Millisecond)
			continue
		}

		// Reset match index if just became a leader
		if !wasLeader {
			rf.mu.Lock()
			wasLeader = true
			// never try to send the 0th index as every server has it
			rf.nextIndex[index] = max(1, len(rf.logs)-1)
			rf.mu.Unlock()
		}

		// Keep waiting until there is a new log to append
		for {
			rf.mu.Lock()
			nextIndexToSend := rf.nextIndex[index]
			lenLogs := rf.getLogLen()
			rf.mu.Unlock()
			// Restart loop if no longer leader
			if !rf.isLeader() {
				continue MainLoop
			}
			// Check if we have the logs for the next index to send
			if nextIndexToSend < lenLogs {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}

		rf.mu.Lock()
		if rf.nextIndex[index] <= rf.snapshotInfo.LastIncludedIndex {
			annotate(rf.me, fmt.Sprintf("Installing snapshot on %v", index), fmt.Sprintf("lastIncludedIndex: %v, lastIncludedTerm: %v", rf.snapshotInfo.LastIncludedIndex, rf.snapshotInfo.LastIncludedTerm))
			snapshot := rf.persister.ReadSnapshot()
			args := InstallSnapshotArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				SnapshotInfo: rf.snapshotInfo,
				Snapshot:     snapshot,
			}
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()
			for !rf.peers[index].Call("Raft.InstallSnapshot", args, reply) && rf.isLeader() {
			}
			// send snapshot until successful
			// if received term greater then step down and continue
			// the code after this if should never be executed, it should be continued either time.Wednesday

			continue MainLoop
		}

		// Prepare args and reply objects
		curLen := rf.getLogLen()
		prevLogIndex := rf.nextIndex[index] - 1
		var prevLogTerm int
		if rf.index(prevLogIndex) >= 0 {
			prevLogTerm = rf.logs[rf.index(prevLogIndex)].Term
		} else if rf.index(prevLogIndex) == -1 {
			prevLogTerm = rf.snapshotInfo.LastIncludedTerm
		} else {
			log.Panicf("rf.index(prevLogIndex) < -1")
		}
		args := &AppendEntryArgs{
			Term:               rf.currentTerm,
			LeaderId:           rf.me,
			PrevLogChronoIndex: prevLogIndex,
			PrevLogTerm:        prevLogTerm,
			Logs:               rf.logs[rf.index(rf.nextIndex[index]):rf.index(curLen)],
			LeaderCommit:       min(rf.matchIndex[index], rf.commitIndex),
		}
		reply := &AppendEntryReply{}
		annotate(rf.me, fmt.Sprintf("Appending to %v", index), fmt.Sprintf("PrevLogChronoIndex: %v, PrevLogTerm: %v, Mine: %v, Sending: %v", args.PrevLogChronoIndex, args.PrevLogTerm, rf.logs, args.Logs))
		rf.mu.Unlock()

		for !rf.peers[index].Call("Raft.AppendEntry", args, reply) {
			if !rf.isLeader() {
				continue MainLoop
			}
		}
		if !rf.isLeader() {
			continue MainLoop
		}

		if rf.termIsGreater(reply.Term) && reply.Success {
			log.Fatalf("Impossible state 569: reply.Term > rf.currentTerm && reply.Success")
		}

		if rf.termIsGreater(reply.Term) {
			rf.stepDown(reply.Term)
			continue
		}

		// Since our term is greater and we are the leader, the failure has occurred because
		// the prev log did not match. So let's go one step back.
		rf.mu.Lock()
		if !reply.Success {
			oldIndex := rf.nextIndex[index]
			if reply.XLogLen > 0 {
				rf.nextIndex[index] = reply.XLogLen
			} else {
				i := rf.getLogLen() - 1
				for ; rf.index(i) >= 0 && rf.logs[rf.index(i)].Term != reply.XTerm; i -= 1 {
				}
				if rf.index(i) >= 0 && rf.logs[rf.index(i)].Term == reply.XTerm {
					rf.nextIndex[index] = i
				} else {
					rf.nextIndex[index] = reply.XIndex
				}
			}
			annotate(rf.me, fmt.Sprintf("Updated NextIndex for %v", index), fmt.Sprintf("Old Index: %v, New Index: %v	\nMy Logs: %v", oldIndex, rf.nextIndex[index], rf.logs))
		} else {
			annotate(rf.me, fmt.Sprintf("Synced up to %v with %v", curLen-1, index), "")
			// Matched until curlen-1 since that is the last index we sent and it was successful
			rf.matchIndex[rf.me] = curLen - 1
			rf.matchIndex[index] = curLen - 1
			// Next index to send will be matchIndex+1
			rf.nextIndex[index] = rf.matchIndex[index] + 1

			copy := slices.Clone(rf.matchIndex)
			slices.Sort(copy)
			// If 5 peers, gives 2. if 6 peers, gives 3, and so on
			majority := int(math.Ceil(float64(len(rf.peers)-1) / 2))
			newCommitIndex := copy[majority]
			if rf.logs[rf.index(curLen-1)].Term == rf.currentTerm {
				rf.commitIndex = max(rf.commitIndex, newCommitIndex)
			}
		}
		rf.mu.Unlock()
		rf.persistLock(nil)
		// check if commitindex can be changed and commit if so
		// if matchIndex[index] != len(rf.logs)-1 {
		// 	args.Logs = rf.logs[nextIndex[index]:]
		// }
	}
}

func (rf *Raft) committer() {
	rf.mu.Lock()
	prevCommitIndex := rf.commitIndex
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		curCommitIndex := rf.commitIndex
		rf.mu.Unlock()
		if curCommitIndex <= prevCommitIndex {
			time.Sleep(3 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		for _, entry := range rf.logs[rf.index(prevCommitIndex+1):rf.index(curCommitIndex+1)] {
			annotate(rf.me, fmt.Sprintf("Committing {index: %v value: %v}", entry.Index, entry.Entry), "")
			applyMsg := raftapi.ApplyMsg{
				Command:      entry.Entry,
				CommandValid: true,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg
		}
		prevCommitIndex = curCommitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) setup() {
	time.Sleep(5 * time.Millisecond)
	rf.mu.Lock()
	// if rf.commitIndex == -1 {
	// 	applyMsg := raftapi.ApplyMsg{
	// 		Command:      rf.logs[0].Entry,
	// 		CommandValid: true,
	// 		CommandIndex: rf.logs[0].Index,
	// 	}
	// 	fmt.Printf("Sending")
	// 	rf.applyCh <- applyMsg
	// 	fmt.Printf("Sent")
	// 	rf.commitIndex = 0
	// }
	rf.mu.Unlock()

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
		applyCh:   applyCh,

		commitIndex: -1,
		currentTerm: 0,
		status:      Follower,
		votedFor:    -1,
		rpcReceived: true,

		logs:       []EntryLog{{Index: 0, Term: 0, Entry: nil}},
		matchIndex: make([]int, len(peers)),
		nextIndex:  make([]int, len(peers)),

		snapshotInfo: SnapshotInfo{
			LastIncludedIndex: -1,
			LastIncludedTerm:  -1,
		},
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeats()
	go rf.updates()
	go rf.committer()
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
