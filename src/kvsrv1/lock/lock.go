package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	client   string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	// You may add code here
	lk := &Lock{ck: ck, lockname: lockname, client: kvtest.RandValue(8)}
	for lk.ck.Put(lockname, "", 0) == rpc.ErrMaybe {
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		owner, version, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			panic(fmt.Errorf("Impossible state: Lock %v not in server", lk.lockname))
		}
		if owner == lk.client {
			return
		}
		if owner != "" {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if lk.ck.Put(lk.lockname, lk.client, version) == rpc.OK {
			return
		}
	}
}

func (lk *Lock) Release() {
	for {
		owner, version, _ := lk.ck.Get(lk.lockname)
		if owner != lk.client {
			break
		}
		lk.ck.Put(lk.lockname, "", version)
	}
}
