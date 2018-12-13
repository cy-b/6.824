package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	// added 
	GET 	 = "Get"
	PUT 	 = "Put"
	APPEND   = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId 	int64
	Cntr		int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key 	string
	ClerkId	int64
	Cntr	int
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
