package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

import "bytes"
import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	// current leader
	leaderId int

	// channel to send command
	applyCh chan ApplyMsg

	// channel to for signals to restart timeout 
	followCh chan rune
	// random generator
	ran *rand.Rand
	// channel for killing the process
	killCh chan struct{}

	// log stuff
	log         []LogEntry
	lastApplied int
	commitIndex int
	applyMu		sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	// locking here to make sure that term and isleader are consistent
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.leaderId == rf.me

	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// *** must be called with rf locked ***
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(len(rf.log))
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
// *** must be called with rf locked ***
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var stateCurrentTerm int
	var stateVotedFor int
	var stateLogLen int

	if d.Decode(&stateCurrentTerm) != nil ||
		d.Decode(&stateVotedFor) != nil ||
		d.Decode(&stateLogLen) != nil {
		// fmt.Println("Error reading persist data")
	} else {
		rf.currentTerm = stateCurrentTerm
		rf.votedFor = stateVotedFor
		tmp := make([]LogEntry, stateLogLen)
		if d.Decode(&tmp) != nil {
			// fmt.Println("Error reading persist logs")
		} else {
			rf.log = tmp
		}
	}	
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term       int
	// Index      int
	Operations interface{}
}

type AppendEntriesArgs struct {
	// leader's term
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// new entries to append after prev log
	Entries      []LogEntry
	// leader's commit index
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term 				int
	Success 			bool
	LastIndexOfTerm		int
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.VoteGranted = false
	// check if the request is new
	if args.Term > rf.currentTerm ||
		(args.Term == rf.currentTerm && 
			(rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {

		// clear leader Id and vote if the message comes from a newer term
		if args.Term > rf.currentTerm {
			rf.leaderId = -1
			rf.votedFor = -1
		}
		// update terms no matter endorsing the request or not
		rf.currentTerm = args.Term

		// check if this candidate has up-to-date logs
		// i.e. the leader election restraints in Ch 5.4
		if (len(rf.log) == 0) ||
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log) - 1) {
			// enforse the request
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			// fmt.Printf("\tP%d votes P%d in T%d\n", rf.me, args.CandidateId, args.Term)
			go func() { rf.followCh <- 1 }()
		} else if args.Term > rf.currentTerm{
			go func() { rf.followCh <- 1 }()
		} else {
		}
	} else {
	}
	// bring the candidate up to date
	reply.Term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	return
}

// Applier for Raft instances 
// Apply the log entries up to the input index
// this makes sure that entries are applied in order 
// and each entry is applied exactly once
func (rf *Raft) ApplyLog(end int) {
	rf.applyMu.Lock()
	if end > rf.lastApplied {
		for i:=rf.lastApplied+1; i <= end; i++ {
			applymsg := ApplyMsg{
							CommandValid: true,
							Command:      rf.log[i].Operations,
							CommandIndex: i+1}
			rf.applyCh <- applymsg
		}
		rf.lastApplied = end
	}
	rf.applyMu.Unlock()
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.LastIndexOfTerm = -1
	if args.Term >= rf.currentTerm {
		// restart timeout
		go func() { rf.followCh <- 1 }()
		// up-to-date heartbeat
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		// implicitly vote for the current leader
		rf.votedFor = args.LeaderId

		// check entries i.e. if PrevLogIndex and PrevLogTerm match
		// if match, update accordingly, else still reply false
		if args.PrevLogIndex < len(rf.log) &&
			(args.PrevLogIndex == -1 ||
				rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {

			// success HB, i.e. matching previous log entry
			// follow exactly from Figure 2.
			if len(rf.log) <= args.PrevLogIndex + 1 + len(args.Entries) {
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
			} else {
				for i:=0; i < len(args.Entries); i++ {
					j := i + 1 + args.PrevLogIndex
					if rf.log[j].Term != args.Entries[i].Term {
						rf.log = rf.log[:j]
						rf.log = append(rf.log, args.Entries[i:]...)
					}
				}
			}
			

			// fmt.Printf("P%d T%d: updates log from %d to %d from leader %d\n", rf.me, rf.currentTerm, args.PrevLogIndex, len(rf.log) - 1, rf.leaderId)
			if len(rf.log) > 0{
				// fmt.Printf("P%d T%d: log sanity, index %d is %v T%d\n", rf.me, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Operations, rf.log[len(rf.log)-1].Term)
			} else {
				// fmt.Printf("P%d T%d: log sanity, empty log\n", rf.me, rf.currentTerm)
			}
			// update commit index
			toCommit := args.LeaderCommit
			// taking the minimum here, since the logs that the leader sent 
			// may not include up to leaderCommit
			if toCommit > args.PrevLogIndex + len(args.Entries) {
				toCommit = args.PrevLogIndex + len(args.Entries)
			}
			if rf.commitIndex < toCommit {
				// fmt.Printf("P%d T%d: commit index uptate from %d to %d\n", rf.me, rf.currentTerm, rf.commitIndex, toCommit)
				rf.commitIndex = toCommit
			}
			go rf.ApplyLog(rf.commitIndex)
			reply.Success = true
		} else {
			// optimization: find out latest index of the log of this term
			start := args.PrevLogIndex
			if start > len(rf.log) - 1 {
				start = len(rf.log) - 1
			}
			for i:=start; i>=0; i-- {
				if rf.log[i].Term == args.PrevLogTerm {
					reply.LastIndexOfTerm = i
				} else if rf.log[i].Term < args.PrevLogTerm {
					break
				}
			}
			reply.Term = rf.currentTerm
			reply.Success = false
			// fmt.Printf("P%d T%d: unsuc HB, prev log indx:%d, term %d\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.persist()
	rf.mu.Unlock()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	isLeader := rf.me == rf.leaderId
	if isLeader {
		// fmt.Printf("P%dT%d start command %v at index %d\n", rf.me, rf.currentTerm, command, len(rf.log))
		// do everything with true index, i.e. 0-based
		index = len(rf.log)
		term = rf.currentTerm
		newLog := LogEntry{
			Term:		term,
			// index is implicit from array index
			Operations:	command,
		}
		// append the new command to leader's log
		rf.log = append(rf.log, newLog)
		rf.persist()
	}
	rf.mu.Unlock()

	// communicate with service using 1-based index
	return index+1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// fmt.Printf("Killing P%d\n", rf.me)
	close(rf.killCh)
	return
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leaderId = -1
	rf.votedFor = -1

	// log stuff
	// log array is initially empty
	// using 1-based index for communication
	rf.lastApplied = -1
	rf.commitIndex = -1

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.followCh = make(chan rune)
	rf.killCh = make(chan struct{})

	// random generator
	seed := time.Now().UnixNano()
	rf.ran = rand.New(rand.NewSource(seed))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start the consensus procedure
	go func() { rf.follower() }()

	return rf
}

func (rf *Raft) follower() {
	for {
		select {
		// recv'd some some present infomation from some peer
		case <-rf.killCh:
			// fmt.Printf("P%d killed in follower\n", rf.me)
			return
		case <-rf.followCh:
		// time out period should be randomized
		// choose interval to be 300 ms ~ 500 ms, with 100 ms as HB frequency
		case <-time.After(time.Duration(300+rf.ran.Intn(100)) * time.Millisecond):
			// fmt.Printf("P%d T%d timed out\n", rf.me, rf.currentTerm)
			// timeout, become candidate
			rf.candidate()
		}
	}
}

func (rf *Raft) candidate() {
	// need to read channel first
	n := len(rf.peers)
	var rvArgs RequestVoteArgs
	for {
		rf.mu.Lock()
		// make sure no newer message is heard before becoming candidate
		select {
		case <-rf.killCh:
			// fmt.Printf("P%d killed in candidate\n", rf.me)
			rf.mu.Unlock()
			return
		case <-rf.followCh:
			rf.mu.Unlock()
			return
		default:
			// become candidate for the new term
			rf.currentTerm++
			rf.votedFor = rf.me
			// fmt.Printf("P%d(candidate) enters T%d and voted for itself\n", rf.me, rf.currentTerm)

			rvArgs = RequestVoteArgs{
				Term:			rf.currentTerm,
				CandidateId:	rf.me,
				// 0-based index, -1 if there's no log
				LastLogIndex:	len(rf.log) - 1,
			}
			
			// set lastlogterm, differentiate the initial case where there is no log
			if rvArgs.LastLogIndex == -1 {
				// initial term is 0
				rvArgs.LastLogTerm = -1
			} else {
				rvArgs.LastLogTerm = rf.log[rvArgs.LastLogIndex].Term
			}
		}
		rf.mu.Unlock()
		

		// make RequestVote RPCs
		// wait for n/2 acknowledgements (leader election)
		ackCntr := 0
		// mutex for the counter and condition channel of Acks
		var ackMu sync.Mutex
		ackCh := make(chan struct{})

		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rvReply := &RequestVoteReply{}
				if rf.sendRequestVote(i, &rvArgs, rvReply) {
					// if granted, increment count by decrement wait count
					if rvReply.VoteGranted {
						ackMu.Lock()
						ackCntr++
						if ackCntr == n/2 {
							// received enough votes, notify by closing one end of channel
							close(ackCh)
						}
						ackMu.Unlock()
					} else {
						// update term and
						// send messge to channel to make server back to follower
						rf.mu.Lock()
						// only consider message with newer term as up-to-date message from peer
						if rvReply.Term > rf.currentTerm {
							rf.currentTerm = rvReply.Term
							rf.votedFor = -1
							rf.leaderId = -1
							go func() {
								// add back-to-follower token
								rf.followCh <- 1
							}()
						}
						rf.mu.Unlock()
					}
				}
			}(i)
		}
		select {
		// received enough votes, become leader for the current candidate term	
		case <-ackCh:
			rf.leader(rvArgs.Term)
			return
		// received an up-to-date message, go back to follower
		case <-rf.followCh:
			return
		// election timeout, rerun candidate in a newer term
		// timeout using 300-500ms as in follwer, with HB frequency around 100ms
		case <-time.After(time.Duration(300+rf.ran.Intn(100)) * time.Millisecond):
		}
	}
}

func (rf *Raft) leader(term int) {
	// update leader ID
	rf.mu.Lock()
	if term == rf.currentTerm {
		rf.leaderId = rf.me
	} else {
		rf.mu.Unlock()
		return
	}
	log_len := len(rf.log) 
	rf.mu.Unlock()
	n := len(rf.peers)
	nextIndex := make([]int, n)
	matchIndex := make([]int, n)
	
	// set itself as leader, making sure the leader hasn't received a newer message
	for i := 0; i < n; i++ {
		// use 0-based index, -1 means no log is replicated yet
		nextIndex[i] = log_len
		matchIndex[i] = -1
	}

	// fmt.Printf("P%d T%d: becomes leader, finishes initialization\n", rf.me, term)
	// each loop will send Heartbeats to all other processes
	for {
		// Check leadership and send HeartBeat msgs
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < n; i++ {
			// no need to send heartbeat msg to itself
			if i == rf.me {
				continue
			}

			// construct per-server append entries argument
			prevlgterm := 0
			// if the log array is not empty
			if nextIndex[i] > 0 {
				// the log right before the new log entries
				prevlgterm = rf.log[nextIndex[i]-1].Term
			}

			// AppendEntries argument for server i
			aeArgs := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex[i] - 1,
				PrevLogTerm:  prevlgterm,
				Entries:      rf.log[nextIndex[i] : len(rf.log)],
				LeaderCommit: rf.commitIndex,
			}

			go func(i int, arg AppendEntriesArgs) {
				aeReply := &AppendEntriesReply{}
				if rf.sendAppendEntries(i, &arg, aeReply) {
					rf.mu.Lock()
					// check leadership
					if rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					if !aeReply.Success {
						// fail because there is a higher term
						if aeReply.Term > term {
							if aeReply.Term > rf.currentTerm {
								// enter new term, clear leader and voting
								rf.currentTerm = aeReply.Term
								rf.leaderId = -1
								rf.votedFor = -1
								go func() { rf.followCh <- 1 }()
							}
						} else {
							// fail because the nextIndex does not match
							// a successful AppendEntries will have nextIndex[i] - 1 = matchIndex[i]
							// optimization, follower will return the most recent index that matched prevTerm
							if nextIndex[i]-1 > matchIndex[i] {
								iot := aeReply.LastIndexOfTerm
								if iot >= 0 && rf.log[iot].Term == arg.PrevLogTerm {
									if iot > matchIndex[i] {
										nextIndex[i] = iot
									} else {
										// fmt.Printf("2: Should not be here")
									}
								} else {
									var j int
									if aeReply.LastIndexOfTerm == -1 {
										j = arg.PrevLogIndex - 1
									} else {
										// fmt.Printf("3: Should not be here")
										j = iot - 1
									}
									for ; j >=0; j-- {
										if rf.log[j].Term < arg.PrevLogTerm {
											if j > matchIndex[i] {
												nextIndex[i] = j
												break
											} else {
												// fmt.Printf("1: Should not be here")
												nextIndex[i] = matchIndex[i] + 1
												break
											}
										}
									}
									if j == -1 {
										nextIndex[i] = 0
									}
								} 
							}
						}
					} else {
						// successful AppendEntries, we know that the entries are replicated on server i
						if arg.PrevLogIndex+len(arg.Entries) > matchIndex[i] {
							matchIndex[i] = arg.PrevLogIndex + len(arg.Entries)
							nextIndex[i] = matchIndex[i] + 1
						}
					}
					rf.mu.Unlock()

				}
			}(i, aeArgs)
		}
		

		// update leader's commitIndex according to current estimate of
		// replicas match indexes using 0-based indexes
		if len(rf.log)-1 > rf.commitIndex {
			// counting backwards, starting from the highest index
			for index := len(rf.log) - 1; index > rf.commitIndex && rf.log[index].Term == term; index-- {
				count := 0
				for i := 0; i < n && count < n/2+1; i++ {
					if matchIndex[i] >= index || i == rf.me  {
						count++
					}
				}
				// found the highest committed log index
				if count >= n/2+1 {
					rf.commitIndex = index
					// fmt.Printf("P%dT%d: leader commits index %d\n",rf.me, rf.currentTerm, rf.commitIndex)
					// fmt.Printf("\t sanity: Ops:%v, Term %d\n", rf.log[index].Operations, rf.log[index].Term)
					break
				}
			}
		}
		go rf.ApplyLog(rf.commitIndex)
		rf.mu.Unlock()

		select {
		// return from leader will go back to follower
		case <-rf.killCh:
			// fmt.Printf("P%d killed in leader\n", rf.me)
			return
		case <-rf.followCh:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

