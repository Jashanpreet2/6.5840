package rsm

import (
	"fmt"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  uint64
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
	channels map[uint64]chan any
}

func annotate(id int, desc, details string) {
	server := fmt.Sprintf("Server %v", id)
	tester.Annotate(server, desc, details)
	// tester.Annotate(server, fmt.Sprintf("%v: %v", time.Since(startTime).Round(time.Second), desc), details)
}

func (rsm *RSM) watchCommits(applyCh chan raftapi.ApplyMsg) {
	for msg := <-applyCh; true; msg = <-applyCh {
		rsm.mu.Lock()
		if msg.SnapshotValid && msg.CommandValid {
			log.Panicf("msg.SnapshotValid && msg.CommandValid")
		}
		var op Op
		var ok bool
		if op, ok = msg.Command.(Op); !ok {
			log.Panicf("msg.Command.(Op) failed")
		}
		res := rsm.sm.DoOp(op.Req)
		if op.Me != rsm.me {
			rsm.mu.Unlock()
			continue
		}
		rsm.channels[op.Id] <- res
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
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

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
	rsm.mu.Lock()
	op := Op{Me: rsm.me, Id: rsm.reqId, Req: req}
	rsm.reqId += 1
	rsm.channels[op.Id] = make(chan any)
	defer delete(rsm.channels, op.Id)
	_, _, isLeader := rsm.rf.Start(op)
	rsm.mu.Unlock()

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

}
