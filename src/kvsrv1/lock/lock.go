package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.name = "lock:" + lockname

	return lk
}

func (lk *Lock) Acquire() {
	err := lk.ck.Put(lk.name, "t", 0)

	if err == rpc.OK {
		return
	}

	if err == rpc.ErrVersion || err == rpc.ErrMaybe {
		for {
			time.Sleep(100 * time.Millisecond)
			lockStatus, vers, err := lk.ck.Get(lk.name)

			if err != rpc.OK { // no key
				break
			}

			if lockStatus == "t" {
				//fmt.Printf("lock already acquired for %s\n", lk.name)
				continue
			}

			// 2. attempt to acquire the lock
			err = lk.ck.Put(lk.name, "t", vers)

			if err == rpc.OK || err == rpc.ErrMaybe {
				break
			}
			//fmt.Printf("acquire error: %s \n", err)
		}

	}
}

func (lk *Lock) Release() {

	for {
		time.Sleep(100 * time.Millisecond)
		_, vers, err := lk.ck.Get(lk.name)

		if err != rpc.OK {
			//fmt.Println("Key probably doesn't exist")
			break
		}

		err = lk.ck.Put(lk.name, "f", vers)

		if err == rpc.OK {
			break
		}
		//fmt.Printf("Release error: %s \n", err)

	}
}
