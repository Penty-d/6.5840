package lock

import (
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
	lockId   string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	lk.lockId = kvtest.RandValue(16)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, ver, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			// 无锁
			putErr := lk.ck.Put(lk.lockname, lk.lockId, 0)
			if putErr == rpc.OK {
				return
			} // 被抢了
		} else if err == rpc.OK {
			// 有锁
			if value == lk.lockId {
				// 已经是自己的锁了
				return
			} else if value == "" {
				// 锁被释放了
				putErr := lk.ck.Put(lk.lockname, lk.lockId, ver)
				if putErr == rpc.OK {
					return
				} // 被抢了
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		value, ver, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			// value == "" 这里不能通过对value判空来规避errmaybe
			// 自己无锁或锁已被释放
			return
		} else if err == rpc.OK {
			if value == lk.lockId {
				// 自己的锁
				putErr := lk.ck.Put(lk.lockname, "", ver) //释放
				if putErr == rpc.OK {
					return
				}
			} else {
				// 不是自己的锁(已释放但errmaybe或者被别人抢了)，这样操作可以规避errmaybe
				return
			}
		}
	}
}
