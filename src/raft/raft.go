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
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int         // The term when this entry was committed
	Index   int         // The index associated with this entry
	Command interface{} // The command associated with this entry
}

// A Go object implementing a single Raft peer.
type Raft struct {
	debugLogEnabled bool // If true, print debug logs

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()
	applyCh   chan ApplyMsg       // Communicate with the tester or client when we commit an entry

	timeoutCond   *sync.Cond // Conditional variable for coordinating elections
	timeoutIndex  int        // Monotically increasing state to coordinate elctions
	timeoutResult bool       // If true, an election should occur

	state       RaftState // Different state leads to a differet set of rules to follow
	currentTerm int       // Latest term we have seen
	votedFor    int       // Candidate we have voted for. -1 means nil.

	log         []LogEntry // Log entries {Term, Index, Command}
	commitIndex int        // Index of highest log entry known to be committed
	lastApplied int        // Index of highest log entry applied to state machine

	// Leader-only. Reinitialized after election.
	nextIndex  []int // For each peer, index of the next log entry to send
	matchIndex []int // For each peer, index of the highest log entry known to be replicated
}

func (rf *Raft) dlog(format string, a ...interface{}) {
	enabled := rf.debugLogEnabled
	if enabled {
		log.Printf("debug: "+format, a...)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	return term, isLeader
}

// This is not locked! User must ensure lock is held before calling this function
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	rf.dlog(
		"[%d]: info: persisted states. Term: %d, VotedFor: %d, Log Len: %d",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.dlog("[%d]: info: skip recovery since no recovery data", rf.me)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil {
		rf.dlog("[%d]: error: failed to recover currentTerm", rf.me)
	}
	if d.Decode(&votedFor) != nil {
		rf.dlog("[%d]: error: failed to recover votedFor", rf.me)
	}
	if d.Decode(&log) != nil {
		rf.dlog("[%d]: error: failed to recover log", rf.me)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log

	rf.dlog(
		"[%d]: info: recovered states. Term: %d, VotedFor: %d, Log Len: %d",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// If current term is older than sender, we will revert to follower.
// Return true to indicate we're reverted to follower
func (rf *Raft) mayRevertToFollowerLocked(senderCurrentTerm int) bool {
	// If current term is older than sender, we will revert to follower.
	termIsOlder := rf.currentTerm < senderCurrentTerm
	if !termIsOlder {
		return false
	}

	rf.currentTerm = senderCurrentTerm
	rf.votedFor = -1
	rf.state = Follower

	rf.persist()

	return true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's current term
	CandidateId  int // ID of candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Receiver's current term. Candidate need to update itself if this is newer. -1 means nil.
	VoteGranted bool // True means candidate received vote.
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Check for common rules.
	reverted := rf.mayRevertToFollowerLocked(args.Term)
	if reverted {
		rf.dlog("[%d]: info: RequestVote: reverted to follower. Sender: %d", rf.me, args.CandidateId)
	}

	// 2. Withhold vote if sender's term is less than our term.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.dlog(
			"[%d]: info: RequestVote: withheld vote because our term is newer. Sender: %d, Our Term: %d, Sender Term: %d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 3. Withhold vote if we already voted for someone else
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = -1
		reply.VoteGranted = false
		rf.dlog(
			"[%d]: info: RequestVote: withheld vote because we already voted. Sender: %d, Our Chosen Candidate: %d",
			rf.me, args.CandidateId, rf.votedFor)
		return
	}

	// 4. Check sender's log is at least as up-to-date as ours. If the logs have last entries with different terms,
	//    then the log with the later term is more up-to-date. If the logs end with the same term, then whichever
	//    log is longer is more up-to-date.
	lastLogEntry := rf.log[len(rf.log)-1]
	if lastLogEntry.Term > args.LastLogTerm {
		reply.Term = lastLogEntry.Term
		reply.VoteGranted = false
		rf.dlog(
			"[%d]: info: RequestVote: withheld vote because our log has newer term. "+
				"Last Entries: Sender Term: %d, Our Term: %d.",
			rf.me, args.LastLogTerm, lastLogEntry.Term)
		return
	}

	if lastLogEntry.Term == args.LastLogTerm && lastLogEntry.Index > args.LastLogIndex {
		reply.Term = lastLogEntry.Term
		reply.VoteGranted = false
		rf.dlog(
			"[%d]: info: RequestVote: withheld vote because our log is longer. "+
				"Last Entries: Term: %d, Sender Last Entry Index: %d, Our Last Entry Index: %d.",
			rf.me, args.LastLogTerm, args.LastLogIndex, lastLogEntry.Index)
		return
	}

	// 5. Set ourselves to follower and acknowledge the sender as our leader.
	rf.votedFor = args.CandidateId
	rf.state = Follower

	rf.persist()

	reply.Term = -1
	reply.VoteGranted = true

	// Reset timer since we have granted a vote to the sender.
	rf.timeoutCond.Signal()

	rf.dlog(
		"[%d]: info: RequestVote: granted vote. Sender: %d, Term: %d",
		rf.me, args.CandidateId, rf.currentTerm)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // Leader's current term
	LeaderId     int        // ID of the leader
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // New entries to be applied
	LeaderCommit int        // Leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // Receiver's current term. Leader needs to update itself if this is newer. -1 means nil.
	Success bool // True if receiver contained entry matching prevLogIndex and prevLogTerm

	FirstConflictIndex int // First index out of sync between leader and follower's log. -1 means nil.
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Check for common rules.
	reverted := rf.mayRevertToFollowerLocked(args.Term)
	if reverted {
		rf.dlog("[%d]: info: AppendEntries: reverted to follower. Sender: %d", rf.me, args.LeaderId)
	}

	// 2. Reply false if sender's term is less than our term.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstConflictIndex = -1
		return
	}

	// Reset timer since by now we're confident we have this request from a real leader
	rf.timeoutCond.Signal()

	// 3. Reply false if log doesn't contain an entry at prevLogIndex.
	if len(rf.log) < args.PrevLogIndex+1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		// Our log is out of sync past the end
		reply.FirstConflictIndex = len(rf.log)
		rf.dlog(
			"[%d]: info: AppendEntries: failed because log is too stale. "+
				"Sender: %d, Sender Last Index: %d, First Conflcit Index: %d",
			rf.me, args.LeaderId, args.PrevLogIndex, reply.FirstConflictIndex)

		// We return here to let sender know to send us the older entries to fix up our log
		return
	}

	// 4. If an existing entry conflicts with a new one, delete the existing entry and all that follow it.
	if prevLogEntry := rf.log[args.PrevLogIndex]; prevLogEntry.Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		// Worst case we're out of sync since the first entry in our log
		reply.FirstConflictIndex = 1
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Index == args.PrevLogTerm {
				// Out log is out of sync past the last entry with the same term
				reply.FirstConflictIndex = i
				break
			}
		}

		rf.dlog(
			"[%d]: info: AppendEntries: log is out of sync. "+
				"Sender: %d, Sender's Last Log: {index: %d, term: %d}, "+
				"Conflicting Entry: {index: %d, term: %d}, First Conflict Index: %d",
			rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
			prevLogEntry.Index, prevLogEntry.Term, reply.FirstConflictIndex)

		// We return here to let sender know to send us the older entries to fix up our log
		return
	}

	// 5. Append any new entries not already in the log. Before that, we remove all entries >= PrevLogIndex.
	//    Only after this, our log can be synced with the leader's log.
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	rf.persist()

	// From this point onwards, this request will succeed
	reply.Success = true
	reply.Term = -1
	reply.FirstConflictIndex = -1

	// 6. If leaderCommit is larger than commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	lastLogEntry := rf.log[len(rf.log)-1]
	if lastLogEntry.Index < args.LeaderCommit {
		rf.commitIndex = lastLogEntry.Index
	} else {
		rf.commitIndex = args.LeaderCommit
	}

	// 7. Apply logs to state machine until we catch up to the leader.
	applied := 0
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++

		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = entry.Index
		msg.Command = entry.Command

		rf.applyCh <- msg

		applied++
	}
	rf.dlog("[%d]: info: AppendEntries: applied %d committed entries.", rf.me, applied)

	rf.dlog(
		"[%d]: info: AppendEntries: succeeded. "+
			"Sender: %d, Entries: %d, Our Term: %d, Our Commited Index: %d, Our Log Size: %d",
		rf.me, args.LeaderId, len(args.Entries), rf.currentTerm, rf.commitIndex, len(rf.log))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO: Is returning the following okay when this raft instance is killed?
	if rf.killed() {
		rf.dlog("[%d]: info: Start: we're killed. Abort.", rf.me)
		return -1, -1, false
	}

	if rf.state != Leader {
		rf.dlog("[%d]: info: Start: we're not leader. Abort.", rf.me)
		return -1, -1, false
	}

	index := len(rf.log)
	rf.log = append(rf.log, LogEntry{rf.currentTerm, index, command})

	rf.dlog(
		"[%d]: info: Start: received new entry. Index: %d, Term: %d, Command: %v",
		rf.me, index, rf.currentTerm, command)

	rf.persist()
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// 1. Prepare for election.
	//   - Increment our current term.
	//   - Vote for ourselves.
	//   - Construct RequestVoteArgs.
	//   - Reset election timer.
	rf.mu.Lock()
	incomingIndex := rf.timeoutIndex
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	numPeers := len(rf.peers)
	lastEntry := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastEntry.Index, lastEntry.Term}
	me := rf.me

	rf.dlog("[%d]: info: Election: starting. Term: %d, Timer Round: %d", rf.me, rf.currentTerm, incomingIndex)

	rf.persist()

	rf.mu.Unlock()

	// 2. Send RequestVote RPCs to all other servers.
	ch := make(chan RequestVoteReply)
	for i := 0; i < numPeers; i++ {
		// We do not need to request vote from ourselves.
		if me == i {
			continue
		}

		go func(peerId int) {
			rf.dlog("[%d]: info: Election: requesting for vote. Peer: %d", me, peerId)

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerId, &args, &reply)

			// Send the vote result to be processed.
			ch <- reply

			if !ok {
				rf.dlog("[%d]: error: Election: request failed. Peer: %d", me, peerId)
				return
			}

			rf.mu.Lock()

			outgoingIndex := rf.timeoutIndex
			// If index mismatch, the current timer has already reset. This means we'll
			// simply ignore the result of the elections.
			if incomingIndex != outgoingIndex {
				rf.dlog(
					"[%d]: info: Election: timed out. Peer: %d, Old Timer Round: %d, New Timer Round: %d",
					me, peerId, incomingIndex, outgoingIndex)
				rf.mu.Unlock()
				return
			}

			// If we have found a higher term, we revert back to being a follower.
			if reply.Term > args.Term {
				rf.dlog(
					"[%d]: info: Election: reverted to follower because reply contains higher term. "+
						"Peer: %d, Our Term: %d, Peer Term: %d",
					me, peerId, args.Term, reply.Term)

				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = args.Term

				rf.persist()

				// We also need to reset the election timer
				rf.timeoutResult = false
				rf.mu.Unlock()
				rf.timeoutCond.Signal()
				return
			}

			rf.mu.Unlock()
		}(i)
	}

	// 3. Check results and determine outcome of the election.
	// We start with one initial vote since we have already voted for ourselves.
	votes := 1
	for i := 1; i < numPeers; i++ {
		reply := <-ch
		if reply.VoteGranted {
			votes++
		}
		if votes > numPeers/2 {
			rf.dlog("[%d]: info: Election: stopping the count with %d votes.", me, votes)
			break
		}
	}

	rf.mu.Lock()
	outgoingIndex := rf.timeoutIndex

	// Fail election and revert to follower if
	// 	(a). Index mismatch: the current election has already expired.
	// 	(b). We failed to secure the majority of the votes.
	if incomingIndex != outgoingIndex ||
		votes <= numPeers/2 {
		rf.dlog(
			"[%d]: info: Election: failed with %d votes. Old Timer Round: %d, New Timer Round: %d",
			me, votes, incomingIndex, outgoingIndex)

		// Reverting back to follower since election failed.
		rf.state = Follower
		rf.votedFor = -1

		rf.persist()

		rf.mu.Unlock()
		return
	}

	rf.dlog("[%d]: info: Election: succeeded with %d votes.", me, votes)

	// We have secured majority of the votes, we'll become leader now and send out heartbeats
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// We also need to reset the election timer
	rf.timeoutResult = false
	rf.mu.Unlock()
	rf.timeoutCond.Signal()
	go rf.commitLogEntries()
	go rf.sendHeartbeats()
}

func (rf *Raft) commitLogEntries() {
	for !rf.killed() {
		rf.mu.Lock()

		// We're no longer leader:
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		indexMap := make(map[int]int)
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}

			if rf.commitIndex < rf.matchIndex[i] {
				indexMap[rf.matchIndex[i]]++
			}
		}

		var indices []int
		for k := range indexMap {
			indices = append(indices, k)
		}
		sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })

		for _, k := range indices {
			if indexMap[k]+1 > len(rf.peers)/2 && rf.log[k].Term == rf.currentTerm {
				oldIndex := rf.commitIndex
				rf.commitIndex = k
				rf.dlog("[%d]: catching up from old commit: %d to new commit: %d", rf.me, oldIndex, rf.commitIndex)
				for i := oldIndex + 1; i <= rf.commitIndex; i++ {
					rf.dlog("[%d]: applying command: %v \n", rf.me, rf.log[i])
					rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandValid: true, CommandIndex: rf.log[i].Index}
				}
			}
		}

		// Catch up on the log every 100ms or so
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	incomingIndex := rf.timeoutIndex
	numPeers := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	for i := 0; i < numPeers; i++ {
		// We do not need to send heartbeat to ourselves.
		if me == i {
			continue
		}

		go func(peerId int) {
			rf.mu.Lock()
			replicationEndIndex := len(rf.log) - 1
			prevLogIndex := rf.nextIndex[peerId] - 1
			prevLogEntry := rf.log[prevLogIndex]
			args := AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
				prevLogEntry.Index,
				prevLogEntry.Term,
				rf.log[prevLogIndex+1 : len(rf.log)],
				rf.commitIndex}

			rf.dlog(
				"[%d]: info: Heartbeat: sending. Peer: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %d, Log Len: %d",
				me, peerId, prevLogIndex, prevLogEntry.Term, len(args.Entries), len(rf.log))
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerId, &args, &reply)

			if !ok {
				rf.dlog("[%d]: error: Heartbeat: request failed. Peer: %d", me, peerId)
				return
			}

			rf.mu.Lock()

			outgoingIndex := rf.timeoutIndex
			// If index mismatch, the current timer has already reset. This means we'll
			// simply ignore the result of these heartbeat replies.
			if incomingIndex != outgoingIndex {
				rf.dlog(
					"[%d]: info: Heartbeat: timed out. Peer: %d, Old Timer Round: %d, New Timer Round: %d",
					me, peerId, incomingIndex, outgoingIndex)
				rf.mu.Unlock()
				return
			}

			// If we have found a higher term, we revert back to being a follower.
			if reply.Term > args.Term {
				rf.dlog(
					"[%d]: info: Heartbeat: reverted to follower because reply contains higher term. "+
						"Peer: %d, Our Term: %d, Peer Term: %d",
					me, peerId, args.Term, reply.Term)

				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = args.Term

				rf.persist()

				// We also need to reset the timer
				rf.timeoutResult = false
				rf.mu.Unlock()
				rf.timeoutCond.Signal()
				return
			}

			// Heartbeat failed due to log mismatch.
			if !reply.Success {
				// We need to decrement nextIndex and retry later. We will repeat this until the follower
				// can reconcile their log with ours.
				if reply.FirstConflictIndex != -1 {
					rf.nextIndex[peerId] = reply.FirstConflictIndex
				} else {
					rf.nextIndex[peerId]--
				}

				rf.dlog(
					"[%d]: info: Heartbeat: failed due to log mismatch. "+
						"Peer: %d, First Conflict Index: %d, New Next Index: %d",
					me, peerId, reply.FirstConflictIndex, rf.nextIndex[peerId])
				rf.mu.Unlock()
				return
			}

			// Heart beat succeeded. We now adjust log entries mapping. We may choose to advance our commit
			// index if an entry has been replicated to the majority of the peers.
			rf.matchIndex[peerId] = replicationEndIndex
			rf.nextIndex[peerId] = replicationEndIndex + 1

			// TODO
			// if replicationEndIndex > rf.commitIndex {
			// 	replicated := 1
			// 	for j := 0; j < len(rf.matchIndex); j++ {
			// 		if me == j {
			// 			continue
			// 		}
			// 		if replicationEndIndex <= rf.matchIndex[j] && rf.currentTerm == rf.log[replicationEndIndex].Term {
			// 			replicated++
			// 		}
			// 	}

			// 	if replicated > len(rf.peers)/2 && rf.currentTerm == rf.log[replicationEndIndex].Term {
			// 		rf.commitIndex = replicationEndIndex
			// 	}
			// }

			rf.dlog(
				"[%d]: info: Heartbeat: succeeded. Peer: %d, Our Commit Index: %d, Our Term: %d",
				me, peerId, rf.commitIndex, rf.currentTerm)
			rf.mu.Unlock()
		}(i)
	}

	// TODO
	// // Apply logs to state machine to catch up to commit index
	// rf.mu.Lock()
	// applied := 0
	// for rf.lastApplied < rf.commitIndex {
	// 	rf.lastApplied++

	// 	entry := rf.log[rf.lastApplied]
	// 	msg := ApplyMsg{}
	// 	msg.CommandValid = true
	// 	msg.CommandIndex = entry.Index
	// 	msg.Command = entry.Command

	// 	rf.applyCh <- msg

	// 	applied++
	// }
	// rf.dlog("[%d]: info: Heartbeat: applied %d committed entries.", me, applied)
	// rf.mu.Unlock()
}

func (rf *Raft) timeoutHandler() {
	rf.mu.Lock()
	isLeader := rf.state == Leader
	incomingIndex := rf.timeoutIndex
	rf.mu.Unlock()

	var sleepDuration int
	if isLeader {
		sleepDuration = 200 // heartbeat timeout
	} else {
		sleepDuration = 400 + rand.Intn(400) // election timeout
	}

	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)

	rf.mu.Lock()
	outgoingIndex := rf.timeoutIndex

	// Timer can be restarted when elections fail or heartbeats downgrade us back to follower. If
	// the timer has been restarted, then this logic no longer needs to wake up the stateHandler
	// goroutine. This is because the stateHandler was already woken up by someone else and has
	// kicked off a new timer already.
	if incomingIndex != outgoingIndex {
		rf.mu.Unlock()
		return
	}
	rf.timeoutResult = true
	rf.mu.Unlock()
	rf.timeoutCond.Signal()
}

func (rf *Raft) stateHandler() {
	// Make sure you reset your election timer exactly when Figure 2 says you should. Specifically,
	// you should only restart your election timer if a) you get an AppendEntries RPC from the
	// current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not
	// reset your timer); b) you are starting an election; or c) you grant a vote to another peer.
	for !rf.killed() {
		rf.mu.Lock()

		rf.timeoutIndex++
		rf.timeoutResult = false

		rf.dlog("[%d]: info: Timer: starting a new one. Timer Round: %d", rf.me, rf.timeoutIndex)

		go rf.timeoutHandler()

		rf.timeoutCond.Wait()
		if rf.timeoutResult {
			rf.dlog("[%d]: info: Timer: fired. Timer Round: %d, State: %d", rf.me, rf.timeoutIndex, rf.state)
			if rf.state == Follower || rf.state == Candidate {
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
				go rf.sendHeartbeats()
			}
		} else {
			rf.dlog("[%d]: info: Timer: reset. Timer Round: %d", rf.me, rf.timeoutIndex)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) DumpLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dlog("%v", rf.log)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	// Debug log enabled?
	// rf.debugLogEnabled = true
	rf.debugLogEnabled = false

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.timeoutCond = sync.NewCond(&rf.mu)
	rf.timeoutIndex = 0
	rf.timeoutResult = false

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 1)
	// Initialize first entry to no-op state
	rf.log[0].Index = 0
	rf.log[0].Term = 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections and heartbeats
	go rf.stateHandler()

	return rf
}
