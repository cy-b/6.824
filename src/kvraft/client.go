package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
// import "fmt"
import "time"
import "sync"


type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	// "uniquely" identifies the Clerk
	clerkId		int64
	// serial number for each command
	cntr		int
	mu 			sync.Mutex
	leaderId	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	// assign unique identifier of each operation
	args := GetArgs{
		Key:		key,
		ClerkId:	ck.clerkId,
		Cntr: 		ck.cntr,
	}
	ck.cntr++
	ck.mu.Unlock()

	

	// request each server if previous one does not work
	for i,n := ck.leaderId,len(ck.servers); ;i = (i+1)%n {
		reply := GetReply{}
		c := make(chan rune)
		go func(sid int, rpl *GetReply) {
			if ck.servers[i].Call("KVServer.Get", &args, rpl) {
				c <- 1
			} else {
				c <- 0
			}
		}(i, &reply)
		select {
		case r := <-c:
			if r == 1 && !reply.WrongLeader {
				switch reply.Err {
				case OK:
					return reply.Value
				case ErrNoKey:
					return ""
				default:
					//fmt.Printf("1 Should not be here \n")
				}
			}
		// in steady state: should be 100ms + 2msg delay + process speed
		case <-time.After(400 * time.Millisecond):
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	// assign unique identifier of each operation
	args := PutAppendArgs{
		Key:		key,
		Value:		value,
		Op:			op,
		ClerkId:	ck.clerkId,
		Cntr: 		ck.cntr,
	}
	ck.cntr++
	ck.mu.Unlock()

	

	for i,n := ck.leaderId,len(ck.servers); ;i = (i+1)%n {
		//fmt.Printf("Ask leader %v\n", i)
		reply := PutAppendReply{}
		c := make(chan rune)
		go func(sid int, rpl *PutAppendReply, ch *chan rune) {
			if ck.servers[i].Call("KVServer.PutAppend", &args, rpl) {
				c <- 1
			} else {
				c <- 0
			}
		}(i, &reply, &c)
		select {
		case r := <-c:
			// //fmt.Printf("got r%v", r)
			if r == 1 && !reply.WrongLeader {
				//fmt.Printf("P%d committed %v\n", i, args)
				switch reply.Err {
				case OK:
					return
				default:
					//fmt.Printf("2 Should not be here \n")
				}
			} 
			// time.Sleep(5*time.Millisecond)
		// in steady state: should be 100ms + 2msg delay + process speed
		case <-time.After(400 * time.Millisecond):
			//fmt.Printf("Waiting in PutAppend\n")
		}
	}
	return 
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
