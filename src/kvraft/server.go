package raftkv

import (
	"labgob"
	"labrpc"
	// "log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType 	string
	Key 	string
	Value 	string
	ClerkId int64
	Cntr 	int
}

type Client struct {
	// keep info about each client
	cntr		int
	reply 		string
	executed 	int
}

type OpLogEntry	struct {
	operation	Op
	result 		string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// added
	clients map[int64]*Client
	logs 	[]OpLogEntry
	db 		map[string]string
	killCh	chan rune
	conditions	[]*sync.Cond 
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Getting %v\n", args)
	// Your code here.
	kv.mu.Lock()
	_, ok := kv.clients[args.ClerkId]
	// no such client before
	if !ok {
		kv.clients[args.ClerkId] = &Client {
			cntr:	args.Cntr,
			reply:		"",
			executed:	-1,
		}
	}

	// ignore request with small count
	if kv.clients[args.ClerkId].cntr > args.Cntr {
		kv.mu.Unlock()
		return
	} else {
		// update counter
		kv.clients[args.ClerkId].cntr = args.Cntr
		// if the command is already executed
		if kv.clients[args.ClerkId].cntr == kv.clients[args.ClerkId].executed {
			// ConstructReply(args, reply, c.reply)
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = kv.clients[args.ClerkId].reply
			kv.mu.Unlock()
			return
		} else {
			// ask raft to commit new Op
			operation := Op {
				OpType:	GET,
				Key:	args.Key,
				// Value?
				ClerkId:args.ClerkId,
				Cntr:	args.Cntr,
			}
			index, _, isLeader := kv.rf.Start(operation)
			if !isLeader {
				// this raft peer is not the leader
				reply.WrongLeader = true
				kv.mu.Unlock()
				return
			} else {
				// create condition variable for this index to wait for
				if len(kv.conditions) < index {
					kv.conditions = append(kv.conditions, make([]*sync.Cond, index - len(kv.conditions))...)
				}
				if kv.conditions[index-1] == nil {
					m := sync.Mutex{}
					kv.conditions[index-1] = sync.NewCond(&m)
				}
				kv.conditions[index-1].L.Lock()
				kv.mu.Unlock()
				kv.conditions[index-1].Wait()
				// the log at this index is committed
				kv.conditions[index-1].L.Unlock()
				if operation == kv.logs[index-1].operation {
					// ConstructReply(args, reply, kv.logs[index-1].result)
					reply.WrongLeader = false
					reply.Err = OK
					reply.Value = kv.logs[index-1].result
					DPrintf("P%d GET(commit) %v, dict:%v\n", kv.me, args, kv.db)
					return
				} else {
					reply.WrongLeader = true
					return
				}
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("P%d got PutAppending %v\n", kv.me, args)
	// Your code here.
	kv.mu.Lock()
	_, ok := kv.clients[args.ClerkId]
	// no such client before
	if !ok {
		kv.clients[args.ClerkId] = &Client {
			cntr:	args.Cntr,
			reply:		"",
			executed:	-1,
		}
		
	}

	// ignore request with small count
	if kv.clients[args.ClerkId].cntr > args.Cntr {
		kv.mu.Unlock()
		return
	} else {
		// update counter
		kv.clients[args.ClerkId].cntr = args.Cntr
		// if the command is already executed
		if kv.clients[args.ClerkId].cntr == kv.clients[args.ClerkId].executed {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		} else {
			// ask raft to commit new Op
			operation := Op {
				OpType:	args.Op,
				Key:	args.Key,
				Value:	args.Value,
				ClerkId:args.ClerkId,
				Cntr:	args.Cntr,
			}
			index, _, isLeader := kv.rf.Start(operation)
			if !isLeader {
				// this raft peer is not the leader
				reply.WrongLeader = true
				kv.mu.Unlock()
				DPrintf("P%d Not leader for PutAppending %v\n", kv.me, args)
				return
			} else {
				DPrintf("P%d(%v) leader K:%v,V:%v,C:%v,cntr:%v \n", kv.me, args.Op,args.Key, args.Value, args.ClerkId, args.Cntr)
				// create condition variable for this index to wait for
				if len(kv.conditions) < index {
					kv.conditions = append(kv.conditions, make([]*sync.Cond, index - len(kv.conditions))...)
				}
				if kv.conditions[index-1] == nil {
					m := sync.Mutex{}
					kv.conditions[index-1] = sync.NewCond(&m)
				}
				kv.conditions[index-1].L.Lock()
				kv.mu.Unlock()
				kv.conditions[index-1].Wait()
				// the log at this index is committed
				kv.conditions[index-1].L.Unlock()
				if operation == kv.logs[index-1].operation {
					reply.WrongLeader = false
					reply.Err = OK
					DPrintf("P%d %v(commit) %v, dict:%v\n", kv.me, args.Op, args, kv.db)
					return
				} else {
					reply.WrongLeader = true
					return
				}
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killCh)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clients = make(map[int64]*Client)
	kv.db 	   = make(map[string]string)
	kv.killCh  = make(chan rune)
	go kv.ApplyChHandler()
	return kv
}

// Handles new ApplyCh responses
func (kv *KVServer) ApplyChHandler() {
	var result string
	var operation Op
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <- kv.applyCh:
			kv.mu.Lock()
			operation = msg.Command.(Op)
			cid, counter := operation.ClerkId, operation.Cntr
			// if the log is not executed, so that each request is executed only once
			_, ok := kv.clients[cid]
			if !ok {
				kv.clients[cid] = &Client {
					cntr:	counter,
					reply:	"",
					executed: -1,
				}
			}
			if kv.clients[cid].executed < counter {
				switch operation.OpType{
				case GET:
					result, _ = kv.db[operation.Key]
				case PUT:
					kv.db[operation.Key] = operation.Value
				case APPEND:
					result, _ = kv.db[operation.Key]
					kv.db[operation.Key] = result + operation.Value
				}
				kv.clients[cid].executed = counter
				// update most recent reply counter
				if kv.clients[cid].cntr <= counter {
					kv.clients[cid].cntr = counter
					kv.clients[cid].reply = result
				}
			} else {
				// this command was already executed, find out its value
				for i:=len(kv.logs)-1; i>=0;i-- {
					if kv.logs[i].operation.ClerkId == cid && kv.logs[i].operation.Cntr == counter {

						result = kv.logs[i].result
						break
					}
				}
			}
			idx := msg.CommandIndex
			if idx > len(kv.logs) {
				// append it to the log it's new
				kv.logs = append(kv.logs, OpLogEntry{
					operation:	operation,
					result:		result,
					})
			} else {
				fmt.Printf("3 Should not be here")
			}
			kv.mu.Unlock()
			// if someone is waiting, wake up
			if len(kv.conditions) >= idx && kv.conditions[idx-1] != nil {
				kv.conditions[idx-1].Broadcast()
			}
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
}
