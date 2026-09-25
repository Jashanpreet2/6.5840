package rsm

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

func NoPrintf(format string, a ...interface{}) {
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int32
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	reqId    uint64
	channels map[int32]chan any
}

func annotate(id int, desc, details string) {
	// server := fmt.Sprintf("Server %v", id)
	// tester.Annotate(server, desc, details)
	// tester.Annotate(server, fmt.Sprintf("%v: %v", time.Since(startTime).Round(time.Second), desc), details)
}

func (rsm *RSM) watchCommits(applyCh chan raftapi.ApplyMsg) {
	for msg := <-applyCh; true; msg = <-applyCh {
		s := time.Now()
		myid := rand.Int()
		rsm.mu.Lock()
		if msg.SnapshotValid && msg.CommandValid {
			log.Panicf("msg.SnapshotValid && msg.CommandValid")
		}
		var op Op = msg.Command.(Op)
		// d := labgob.NewDecoder(bytes.NewBuffer(msg.Command.([]byte)))

		// if err := d.Decode(&op); err != nil {
		// 	log.Panicf("msg.Command.(Op) failed: %v", err)
		// }

		rsm.mu.Unlock()
		res := rsm.sm.DoOp(op.Req)
		rsm.mu.Lock()
		NoPrintf("%v: time from receive commit to doop + lock: %v\n", myid, time.Since(s))

		if op.Me != rsm.me {
		} else if ch, ok := rsm.channels[op.Id]; ok {
			ch <- res
			NoPrintf("%v: time from receive commit to send to ch: %v\n", myid, time.Since(s))
		}
		// for i := nextToSend; i != nil && *i < op.Id; *i++ {
		// 	rsm.channels[op.Id] <- WatchCommitResponse{nil, rpc.ErrWrongLeader}
		// }
		rsm.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		reqId:        0,
		channels:     map[int32]chan any{},
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.watchCommits(rsm.applyCh)
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	// your code here
	s := time.Now()
	defer func() { NoPrintf("Submit total time: %v\n", time.Since(s)) }()
	rsm.mu.Lock()
	id := rand.Int32N(100000)

	// Encode to bytes
	var buf bytes.Buffer
	e := labgob.NewEncoder(&buf)
	op := Op{Me: rsm.me, Id: id, Req: req}
	if err := e.Encode(op); err != nil {
		log.Panic(err)
	}

	NoPrintf("Submit req: %v, type: %T", req, req)

	rsm.reqId += 1
	channel := make(chan any)
	rsm.channels[id] = channel
	defer func() { rsm.mu.Lock(); delete(rsm.channels, id); rsm.mu.Unlock() }()
	rsm.mu.Unlock()
	_, commitTerm, isLeader := rsm.rf.Start(op)
	// NoPrintf("Started in time: %v\n", time.Since(s))

	if !isLeader {
		rsm.mu.Lock()
		annotate(rsm.me, fmt.Sprintf("Failed to start: %v", rsm.reqId), "Not leader")
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			currentTerm, _ := rsm.rf.GetState()
			if currentTerm != commitTerm {
				cancel()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}(ctx)

	for {
		select {
		case res := <-channel:
			cancel()
			return rpc.OK, res
		case <-ctx.Done():
			return rpc.ErrWrongLeader, nil
		default:
			time.Sleep(1 * time.Microsecond)
		}
	}
}
