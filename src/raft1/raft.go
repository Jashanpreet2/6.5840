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
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"slices"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var f, err = os.OpenFile("debug.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

var startTime time.Time

func annotate(id int, desc, details string) {
	server := fmt.Sprintf("Server %v", id)
	tester.Annotate(server, fmt.Sprintf("%v: %v", time.Since(startTime).Round(time.Second), desc), details)
	// if err != nil {
	// 	log.Panic(err)
	// }
	// f.WriteString(fmt.Sprintf("Server: %v\nDesc: %v\nDetaails: %v\n\n", id, desc, details))
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
	committedIndex   int
	commitUntilIndex int
	commitLock       sync.Mutex

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
	Id                 int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLogLen int
}

func (rf *Raft) termIsGreater(term int) bool {
	return term > rf.currentTerm
}

func (rf *Raft) termIsGreaterLock(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.termIsGreater(term)
}

func (rf *Raft) isCandidate() bool {
	return rf.status == Candidate
}

func (rf *Raft) isCandidateLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isCandidate()
}

func (rf *Raft) isLeader() bool {
	return rf.status == Leader
}
func (rf *Raft) isLeaderLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isLeader()
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
	defer rf.persist(nil)
	annotate(rf.me, "Stepping down", fmt.Sprintf("Old term: %v, new term: %v, nextIndex[i]: %v", rf.currentTerm, newTerm, max(1, rf.getLogLen()-1)))
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
			rf.matchIndex[i] = rf.getLogLen() - 1
		} else {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = max(1, rf.getLogLen()-1)
		}
	}
}
func (rf *Raft) stepDownLock(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.stepDown(newTerm)
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	if len(args.Logs) > 0 {
		original := slices.Clone(rf.logs)
		annotate(rf.me, fmt.Sprintf("AppendEntry from %v", args.LeaderId), fmt.Sprintf("Id: %v, Received term: %v. My term: %v", args.Id, args.Term, rf.currentTerm))
		defer func() {
			annotate(rf.me, "AppendEntry permutations", fmt.Sprintf("From: %v\nAdded: %v\nFinal: %v\n     Reply success: %v, term: %v, xloglen: %v, xindex: %v, xterm: %v", original, args.Logs, rf.logs, reply.Success, reply.Term, reply.XLogLen, reply.XIndex, reply.XTerm))
		}()
	} else {
		annotate(rf.me, fmt.Sprintf("HB from %v", args.LeaderId), fmt.Sprintf("Id: %v, Received term: %v. My term: %v", args.Id, args.Term, rf.currentTerm))
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		annotate(rf.me, "AppendEntry rejected", fmt.Sprintf("Args.Term (%v) < rf.currentTerm (%v)", args.Term, rf.currentTerm))
		reply.Success = false
		return
	}
	rf.rpcReceived = true
	if rf.status == Candidate || args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	if args.LeaderCommit > rf.getLogLen() {
		annotate(rf.me, "args.LeaderCommit > rf.getLogLen()", fmt.Sprintf("Leader commit: %v, rf.getLogLen(): %v, snapshotLastIndex: %v, snapshotLastTerm: %v", args.LeaderCommit, rf.getLogLen(), rf.snapshotInfo.LastIncludedIndex, rf.snapshotInfo.LastIncludedTerm))
		log.Panicf("args.LeaderCommit > rf.getLogLen")
	}
	rf.commitUntilIndex = max(args.LeaderCommit, rf.commitUntilIndex)

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
		annotate(rf.me, "PrevLogTerm doesn't match", fmt.Sprintf("My term (index %v): %v,given term: %v", args.PrevLogChronoIndex, prevLogTerm, args.PrevLogTerm))
		return
	}

	if args.Logs[0].Index < rf.commitUntilIndex {
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

var persistTime = false

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if persistTime {
		e.Encode(startTime)
	}
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotInfo)
	e.Encode(rf.committedIndex)
	e.Encode(rf.commitUntilIndex)
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
	var committedIndex int
	var commitUntilIndex int

	var tmpStartTime time.Time
	if persistTime && d.Decode(&tmpStartTime) != nil {
		startTime = tmpStartTime
	}
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotInfo) != nil ||
		d.Decode(&committedIndex) != nil ||
		d.Decode(&commitUntilIndex) != nil {
		log.Panicf("Failed to decode")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotInfo = snapshotInfo
		if rf.snapshotInfo.LastIncludedIndex < rf.committedIndex {
			log.Panicf("rf.snapshotInfo.LastIncludedIndex < rf.committedIndex")
		}
		rf.committedIndex = committedIndex
		rf.commitUntilIndex = commitUntilIndex
	}

	if len(snapshot) == 0 {
		if rf.snapshotInfo.LastIncludedIndex > 0 {
			log.Panicf("lastSnapshotIndex > 0 but snapshot is nil")
		}
		return
	}

	annotate(rf.me, "Received non-nil snapshot", "")
	msg := raftapi.ApplyMsg{SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  rf.snapshotInfo.LastIncludedTerm,
		SnapshotIndex: rf.snapshotInfo.LastIncludedIndex,
	}
	go func() {
		rf.applyCh <- msg
	}()
	annotate(rf.me, "Snapshot applied to applyCh", "")
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) snapshotToMsgs(snapshot []byte) []raftapi.ApplyMsg {
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)

	var lastIncludedIndex int
	var commands []interface{}
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&commands) != nil {
		log.Panicf("Failed to decode snapshot")
	}
	msgs := make([]raftapi.ApplyMsg, lastIncludedIndex+1)
	for i, command := range commands {
		msgs[i] = raftapi.ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: i,
		}
	}
	return msgs
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(chronoIndex int, snapshot []byte) { //data was originally named snapshot
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.index(chronoIndex) >= len(rf.logs) {
		log.Panicf("ArrayIndex(Snapshot index) >= len(rf.logs)")
	}
	// if chronoIndex > rf.commitIndex {
	// 	log.Panicf("Snapshot index > rf.commitIndex")
	// }
	// Accidental snapshot when there is no new info to snapshot
	if len(rf.logs) == 0 {
		return
	}
	if len(snapshot) == 0 {
		log.Panicf("Snapshot() with no data")
	}

	// Chronological index to array index
	arrayIndex := rf.index(chronoIndex)
	if chronoIndex > rf.commitUntilIndex {
		log.Panicf("chronoIndex > rf.commitIndex: Non committed logs in snapshot")
	}
	if chronoIndex > rf.commitUntilIndex {
		log.Panicf("chronoIndex > rf.commitIndex: Non committed logs in snapshot")
	}

	rf.snapshotInfo.LastIncludedIndex = chronoIndex
	rf.snapshotInfo.LastIncludedTerm = rf.logs[arrayIndex].Term
	// msgs := rf.snapshotToMsgs(snapshot)
	// fmt.Printf("Snapshot Messages: %v\n", msgs)
	rf.logs = slices.Clone(rf.logs[arrayIndex+1:])

	annotate(rf.me, "Snapshotted", fmt.Sprintf("lastChronoIndex: %v, lastTerm: %v, new logs: %v", rf.snapshotInfo.LastIncludedIndex, rf.snapshotInfo.LastIncludedTerm, rf.logs))

	rf.persist(snapshot)
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
	myterm := rf.currentTerm
	defer func() {
		annotate(rf.me, fmt.Sprintf("Vote? %v. From %v. Myterm: %v. Their term: %v", reply.VoteGranted, args.CandidateId, myterm, args.Term), desc)
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

	reply.Term = args.Term
	rf.mu.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)

	if rf.termIsGreater(args.Term) {
		rf.stepDown(args.Term)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		desc = "Already voted this term"
		reply.VoteGranted = false
		return
	}
	// Check if their log is at least as up to date as ours
	myLastLogIndex, myLastLogTerm := rf.getLastIndexAndTerm()
	if args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
		desc = "Voted"
		rf.rpcReceived = true
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
	defer rf.mu.Unlock()
	defer rf.persist(nil)

	if rf.status != Leader {
		return -1, -1, false
	}
	prevLogs := rf.logs
	rf.logs = append(rf.logs, EntryLog{
		Index: rf.getLogLen(),
		Term:  rf.currentTerm,
		Entry: command,
	},
	)
	annotate(rf.me, fmt.Sprintf("Start(%v)", command), fmt.Sprintf("Command's index(LogLen): %v, Prev logs: %v, New logs: %v", rf.getLogLen(), prevLogs, rf.logs))

	// command is the entry that needs to be committed
	// the current server needs to be the leader to follow
	// through. if it is not the leader it returns falseit will be 0 indexed.
	// the term is just the current term of the server. the index is the
	// existing last index of the log array+1 i.e. equal to the length
	// of the log array.

	// Your code here (3B).

	return rf.logs[len(rf.logs)-1].Index, rf.currentTerm, true
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.stepDown(rf.currentTerm + 1)
	rf.status = Candidate
	rf.votedFor = rf.me
	votes := 1
	votesNeeded := int(math.Floor(float64(len(rf.peers))/2.)) + 1

	annotate(rf.me, fmt.Sprintf("starting election: %v", rf.currentTerm), "")

	rf.persist(nil)
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
				if votes >= votesNeeded || rf.status != Candidate || rf.currentTerm != term {
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
				rf.mu.Lock()
				if rf.termIsGreater(reply.Term) {
					rf.stepDown(reply.Term)
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
	for rf.isCandidateLock() {
		rf.mu.Lock()
		select {
		case _ = <-timeout:
			rf.stepDown(rf.currentTerm)
			rf.mu.Unlock()
			break Loop
		default:
			// DPrintf("%v startElection hasn't timed out", rf.me)
		}

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
		time.Sleep(10 * time.Millisecond)
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
		for rf.isLeaderLock() {
			time.Sleep(2 * time.Millisecond)
		}

		rf.mu.Lock()
		rf.rpcReceived = false
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 240 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeats() {
	heartbeat := func(index int) {
		if index == rf.me {
			return
		}
		if !rf.isLeaderLock() {
			return
		}

		rf.mu.Lock()
		args := &AppendEntryArgs{
			Term:               rf.currentTerm,
			LeaderId:           rf.me,
			PrevLogChronoIndex: 0,
			PrevLogTerm:        0,
			Logs:               []EntryLog{},
			LeaderCommit:       min(rf.matchIndex[index], rf.commitUntilIndex),
			Id:                 rand.Int(),
		}
		reply := &AppendEntryReply{}
		annotate(rf.me, fmt.Sprintf("Heartbeating %v", index), fmt.Sprintf("Id: %v", args.Id))

		rf.mu.Unlock()
		ok := rf.peers[index].Call("Raft.AppendEntry", args, reply)
		rf.mu.Lock()
		annotate(rf.me, fmt.Sprintf("RPC: Heartbeated %v. Status: %v, term: %v,", index, ok, reply.Term), "")
		if reply.Term > rf.currentTerm && reply.Success {
			log.Fatalf("reply.Term > rf.currentTerm && reply.Success")
		}
		if rf.termIsGreater(reply.Term) {
			rf.stepDown(reply.Term)
		}
		rf.mu.Unlock()
	}

	for {
		time.Sleep(100 * time.Millisecond)
		if !rf.isLeaderLock() {
			continue
		}
		for i := range rf.peers {
			// peer called. caller. response.
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

	rf.commitLock.Lock()
	defer rf.commitLock.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		annotate(rf.me, "Snapshot Rejected. My term is greater", fmt.Sprintf("My term: %v. Args.Term: %v", rf.currentTerm, args.Term))
		return
	}
	rf.rpcReceived = true
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	if args.SnapshotInfo.LastIncludedIndex < rf.snapshotInfo.LastIncludedIndex {
		log.Panicf("args.SnapshotInfo.LastIncludedIndex < rf.snapshotInfo.LastIncludedIndex")
	}
	msg := raftapi.ApplyMsg{SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.SnapshotInfo.LastIncludedTerm,
		SnapshotIndex: args.SnapshotInfo.LastIncludedIndex,
	}

	rf.commitUntilIndex = max(args.SnapshotInfo.LastIncludedIndex, rf.commitUntilIndex)
	rf.committedIndex = args.SnapshotInfo.LastIncludedIndex
	rf.snapshotInfo.LastIncludedIndex = args.SnapshotInfo.LastIncludedIndex
	rf.snapshotInfo.LastIncludedTerm = args.SnapshotInfo.LastIncludedTerm
	rf.logs = []EntryLog{}
	rf.persist(args.Snapshot)
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()

	annotate(rf.me, fmt.Sprintf("Installed snapshot from %v", args.LeaderId), fmt.Sprintf("snapshotInfo lastIndex: %v, lastTerm: %v, my logs: %v", rf.snapshotInfo.LastIncludedIndex, rf.snapshotInfo.LastIncludedTerm, rf.logs))

	//deferred rf.mu.unlock unlocks here
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
MainLoop:
	for {
		// Wait until becoming leader
		if !rf.isLeaderLock() {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// Keep waiting until there is a new log to append
		for {
			rf.mu.Lock()
			nextIndexToSend := rf.nextIndex[index]
			lenLogs := rf.getLogLen()
			rf.mu.Unlock()
			// Restart loop if no longer leader
			if !rf.isLeaderLock() {
				continue MainLoop
			}
			// Check if we have the logs for the next index to send
			if nextIndexToSend < lenLogs {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		rf.mu.Lock()
		term := rf.currentTerm
		var snapshotReply *InstallSnapshotReply = nil
		installSnapshotReplyMutex := sync.Mutex{}
	SnapshotLoop:
		// While the nextindex < snapshotted index and server is leader and the term hasn't changed
		for rf.nextIndex[index] <= rf.snapshotInfo.LastIncludedIndex && rf.isLeader() && rf.currentTerm == term {
			rf.mu.Unlock()
			rf.mu.Lock()
			annotate(rf.me, fmt.Sprintf("Installing snapshot on %v", index), fmt.Sprintf("nextIndex[index]: %v, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.nextIndex[index], rf.snapshotInfo.LastIncludedIndex, rf.snapshotInfo.LastIncludedTerm))
			snapshot := rf.persister.ReadSnapshot()
			ctx, cancel := context.WithCancel(context.Background())
			if len(snapshot) == 0 {
				log.Panicf("Installing snapshot with len 0. rf.nextIndex[index]: %v, snapshot lastIndex: %v", rf.nextIndex[index], rf.snapshotInfo.LastIncludedIndex)
			}

			args := &InstallSnapshotArgs{
				Term:         term,
				LeaderId:     rf.me,
				SnapshotInfo: rf.snapshotInfo,
				Snapshot:     snapshot,
			}

			annotate(rf.me, fmt.Sprintf("RPC: InstallSnapshot %v", index), "")
			rf.mu.Unlock()
			go func(args *InstallSnapshotArgs, ctx context.Context) {
				mySnapshotReply := &InstallSnapshotReply{}
				success := rf.peers[index].Call("Raft.InstallSnapshot", args, mySnapshotReply)
				select {
				case <-ctx.Done():
					return
				default:
					installSnapshotReplyMutex.Lock()
					defer installSnapshotReplyMutex.Unlock()
					if success && snapshotReply == nil {
						snapshotReply = mySnapshotReply
					}
				}
			}(args, ctx)
			// send snapshot until successful
			// if received term greater then step down and continue
			// the code after this if should never be executed, it should be continued either time.Wednesday

			timer := time.NewTimer(400 * time.Millisecond)
			for {
				select {
				case <-timer.C:
					cancel()
					rf.mu.Lock()
					continue SnapshotLoop
				default:
				}

				installSnapshotReplyMutex.Lock()
				if snapshotReply == nil {
					installSnapshotReplyMutex.Unlock()
					time.Sleep(10 * time.Millisecond)
					continue
				} else {
					installSnapshotReplyMutex.Unlock()
					break
				}
			}

			rf.mu.Lock()
			if args.Term < rf.currentTerm {
				rf.mu.Unlock()
				continue MainLoop
			}
			if rf.termIsGreater(snapshotReply.Term) {
				rf.stepDown(snapshotReply.Term)
			} else {
				rf.matchIndex[index] = args.SnapshotInfo.LastIncludedIndex
				rf.nextIndex[index] = rf.matchIndex[index] + 1
			}
			rf.mu.Unlock()

			continue MainLoop
		}

		// Prepare args and reply objects
		curLen := rf.getLogLen()
		var prevLogIndex int
		var prevLogTerm int

		var reply *AppendEntryReply = nil
		var args *AppendEntryArgs = nil
		var replyMutex = sync.Mutex{}

		rf.mu.Unlock()

	AppendLoop:
		for rf.isLeaderLock() {
			ctx, cancel := context.WithCancel(context.Background())
			rf.mu.Lock()
			if rf.nextIndex[index] <= rf.snapshotInfo.LastIncludedIndex {
				annotate(rf.me, fmt.Sprintf("Appending to %v: Exit, nextIndex snapshotted", index), "")
				rf.mu.Unlock()
				continue MainLoop
			}

			prevLogIndex = rf.nextIndex[index] - 1
			if rf.index(prevLogIndex) >= 0 {
				prevLogTerm = rf.logs[rf.index(prevLogIndex)].Term
			} else if rf.index(prevLogIndex) == -1 {
				prevLogTerm = rf.snapshotInfo.LastIncludedTerm
			} else {
				log.Panicf("rf.index(prevLogIndex) < -1")
			}
			curLen = rf.getLogLen()
			args = &AppendEntryArgs{
				Term:               term,
				LeaderId:           rf.me,
				PrevLogChronoIndex: prevLogIndex,
				PrevLogTerm:        prevLogTerm,
				Logs:               rf.logs[rf.index(rf.nextIndex[index]):rf.index(curLen)],
				LeaderCommit:       min(rf.matchIndex[index], rf.commitUntilIndex),
				Id:                 rand.Int(),
			}
			annotate(rf.me, fmt.Sprintf("RPC: Appending to %v", index), fmt.Sprintf("Id: %v, PrevLogChronoIndex: %v, PrevLogTerm: %v, Mine: %v, Sending: %v", args.Id, args.PrevLogChronoIndex, args.PrevLogTerm, rf.logs, args.Logs))
			rf.mu.Unlock()

			go func(args *AppendEntryArgs, ctx context.Context) {
				myReply := &AppendEntryReply{}
				success := rf.peers[index].Call("Raft.AppendEntry", args, myReply)
				select {
				case <-ctx.Done():
					return
				default:
					replyMutex.Lock()
					defer replyMutex.Unlock()
					if success && reply == nil {
						reply = myReply
					}
				}

			}(args, ctx)

			timer := time.NewTimer(400 * time.Millisecond)
			for {
				select {
				case <-timer.C:
					cancel()
					continue AppendLoop
				default:
					replyMutex.Lock()
					if reply != nil {
						replyMutex.Unlock()
						break AppendLoop
					}
					replyMutex.Unlock()
					time.Sleep(10 * time.Millisecond)
				}
			}
			// if reply != nil {
			// 	replyMutex.Unlock()
			// 	break
			// }
			// replyMutex.Unlock()
			// time.Sleep(400 * time.Millisecond)
			// cancel()
		}

		if reply == nil {
			continue MainLoop
		}

		// fmt.Printf("Reply: %v\n", reply)
		// for !rf.peers[index].Call("Raft.AppendEntry", args, reply) && rf.isLeaderLock() {
		// 	rf.mu.Lock() // good
		// 	annotate(rf.me, fmt.Sprintf("RPC: Reappending to %v", index), fmt.Sprintf("Id: %v, PrevLogChronoIndex: %v, PrevLogTerm: %v, Mine: %v, Sending: %v", args.Id, args.PrevLogChronoIndex, args.PrevLogTerm, rf.logs, args.Logs))

		// }
		annotate(rf.me, fmt.Sprintf("Appending to %v: Exited", index), "")
		if !rf.isLeaderLock() {
			continue MainLoop
		}

		rf.mu.Lock()
		if rf.termIsGreater(reply.Term) && reply.Success {
			log.Panicf("Impossible state 569: reply.Term > rf.currentTerm && reply.Success")
		}

		if rf.termIsGreater(reply.Term) {
			rf.stepDown(reply.Term)
			rf.mu.Unlock()
			continue MainLoop
		}

		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			continue MainLoop
		}

		// Since our term is greater and we are the leader, the failure has occurred because
		// the prev log did not match. So let's go one step back.
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
			// fmt.Printf("Synced up to %v with %v", curLen-1, index)
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
			if curLen-1 <= rf.commitUntilIndex {

			} else if rf.logs[rf.index(curLen-1)].Term == rf.currentTerm {
				rf.commitUntilIndex = max(rf.commitUntilIndex, newCommitIndex)
			}
		}
		rf.persist(nil)
		rf.mu.Unlock()
	}
}

var committed = []EntryLog{}

func (rf *Raft) committer() {
	for {
		rf.mu.Lock()
		if rf.committedIndex == rf.commitUntilIndex {
			time.Sleep(3 * time.Millisecond)
			rf.mu.Unlock()
			continue
		}
		if rf.committedIndex > rf.commitUntilIndex {
			log.Panicf("rf.committedIndex > rf.commitUntilIndex")
		}

		// if prevCommitIndex < rf.snapshotInfo.LastIncludedIndex {
		// 	annotate(rf.me, "prevCommit < snapshot last index", fmt.Sprintf("prevCommitIndex: %v, snapshot lastIncludedIndex: %v", prevCommitIndex, rf.snapshotInfo.LastIncludedIndex))
		// 	prevCommitIndex = rf.snapshotInfo.LastIncludedIndex
		// 	rf.mu.Unlock()
		// 	continue
		// }
		commitUntilIndex := rf.commitUntilIndex
		// fmt.Printf("rf.committedIndex: %v, rf.logs: %v, rf.index(rf.committedIndex+1): %v\n", rf.committedIndex, rf.logs, rf.index(rf.committedIndex+1))
		for _, entry := range rf.logs[rf.index(rf.committedIndex+1):rf.index(commitUntilIndex+1)] {
			annotate(rf.me, fmt.Sprintf("Committing {index: %v value: %v}", entry.Index, entry.Entry), fmt.Sprintf("rf.index(prevCommitIndex+1): %v, prevCommitIndex: %v, logs: %v", rf.index(rf.committedIndex+1), rf.committedIndex, rf.logs))
			applyMsg := raftapi.ApplyMsg{
				Command:      entry.Entry,
				CommandValid: true,
				CommandIndex: entry.Index,
			}
			committed = append(committed, entry)
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()

			// InstallSnapshot has committed above the current batch being commieted here
			if rf.committedIndex >= commitUntilIndex {
				break
			}
			rf.committedIndex = entry.Index
			rf.persist(nil)
			annotate(rf.me, "Committed", fmt.Sprintf("All committed: %v", committed))
		}
		if commitUntilIndex < rf.committedIndex {
			log.Panicf("commitUntilIndex < rf.committedIndex")
		}
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

		commitUntilIndex: 0,
		committedIndex:   0,
		commitLock:       sync.Mutex{},

		currentTerm: 0,
		status:      Follower,
		votedFor:    -1,
		rpcReceived: true,

		logs:       []EntryLog{},
		matchIndex: make([]int, len(peers)),
		nextIndex:  make([]int, len(peers)),

		snapshotInfo: SnapshotInfo{
			LastIncludedIndex: 0,
			LastIncludedTerm:  0,
		},
	}
	startTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = max(1, rf.getLogLen()-1)
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
