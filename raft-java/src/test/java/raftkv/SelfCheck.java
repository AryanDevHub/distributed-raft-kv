package raftkv;

import raftkv.Messages.AppendEntriesRequest;
import raftkv.Messages.AppendEntriesResponse;
import raftkv.Messages.RequestVoteRequest;
import raftkv.Messages.RequestVoteResponse;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Dependency-free checks for the properties the design depends on. Run with:
 * java -cp out:out-test raftkv.SelfCheck     (exits non-zero on the first failure)
 */
public final class SelfCheck {

    private static int checks;

    private static void check(boolean condition, String what) {
        checks++;
        if (!condition) {
            throw new AssertionError("FAILED: " + what);
        }
        System.out.println("  ok  " + what);
    }

    private static LogEntry entry(long term, long index) {
        return new LogEntry(term, index, Command.put("k" + index, "v" + index));
    }

    public static void main(String[] args) throws Exception {
        walRecovery();
        hardState();
        quorumFormula();
        voteLogUpToDateRule();
        oldTermEntriesCommitOnlyIndirectly();
        conflictingEntriesAreOverwritten();
        preVote();
        checkQuorum();
        leaderStickiness();
        System.out.println("All " + checks + " checks passed.");
    }

    static void walRecovery() throws IOException {
        System.out.println("BinaryWAL");
        Path dir = Files.createTempDirectory("wal-check");
        Path file = dir.resolve("raft.wal");
        long validSize;
        try (BinaryWAL wal = new BinaryWAL(dir)) {
            check(wal.recoverAll().isEmpty(), "fresh log recovers empty");
            wal.appendAll(List.of(entry(1, 1), entry(1, 2)));
            wal.append(entry(2, 3));
        }
        validSize = Files.size(file);

        // Torn write: a header promising 100 payload bytes, but only 3 arrive before "power loss".
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(BinaryWAL.MAGIC);
            raf.writeInt(100);
            raf.write(new byte[]{1, 2, 3});
        }
        try (BinaryWAL wal = new BinaryWAL(dir)) {
            List<LogEntry> recovered = wal.recoverAll();
            check(recovered.size() == 3, "partial tail frame ignored, 3 valid entries recovered");
            check(Files.size(file) == validSize, "log truncated back to last valid offset");
            wal.append(entry(2, 4));
            wal.truncateSuffix(4);
            check(wal.lastIndex() == 3 && Files.size(file) == validSize, "truncateSuffix removes entries on disk");
        }

        // Bit rot in the last frame's payload: CRC32 mismatch must drop that frame.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length() - 6);
            int b = raf.read();
            raf.seek(raf.length() - 6);
            raf.write(b ^ 0xFF);
        }
        try (BinaryWAL wal = new BinaryWAL(dir)) {
            check(wal.recoverAll().size() == 2, "frame with bad CRC32 dropped, earlier entries kept");
        }
    }

    static void hardState() throws IOException {
        System.out.println("StateStore");
        Path dir = Files.createTempDirectory("state-check");
        StateStore store = new StateStore(dir);
        check(store.load().currentTerm() == 0 && store.load().votedFor() == null, "missing file loads as term 0, no vote");
        store.save(7, "node2");
        store.save(8, null);
        Files.writeString(dir.resolve("hardstate.bin.tmp"), "half-written garbage");
        StateStore.HardState loaded = new StateStore(dir).load();
        check(loaded.currentTerm() == 8 && loaded.votedFor() == null, "latest atomic update wins; stray temp file ignored");
        store.save(9, "node3");
        check(new StateStore(dir).load().votedFor().equals("node3"), "votedFor round-trips");
    }

    static void quorumFormula() {
        System.out.println("Quorum");
        check(new RaftCore("a", List.of("b", "c"), 0, null, List.of()).quorum() == 2, "3-node cluster needs 2");
        check(new RaftCore("a", List.of("b", "c", "d", "e"), 0, null, List.of()).quorum() == 3, "5-node cluster needs 3");
        check(new RaftCore("a", List.of("b", "c", "d"), 0, null, List.of()).quorum() == 3, "4-node cluster needs 3");
        check(new RaftCore("a", List.of(), 0, null, List.of()).quorum() == 1, "single node needs 1");
    }

    static void voteLogUpToDateRule() {
        System.out.println("§5.4.1 vote safety");
        RaftCore core = new RaftCore("n1", List.of("n2", "n3"), 2, null, List.of(entry(2, 1)));
        RaftCore.Outcome<RequestVoteResponse> staleTerm = core.handleRequestVote(new RequestVoteRequest(3, "n2", 5, 1));
        check(!staleTerm.value().voteGranted(), "denied: candidate's last log term is lower despite a longer log");
        check(staleTerm.actions().get(0) instanceof Action.PersistHardState, "term bump persisted first");

        RaftCore.Outcome<RequestVoteResponse> shorter = core.handleRequestVote(new RequestVoteRequest(3, "n2", 0, 2));
        check(!shorter.value().voteGranted(), "denied: same last term but shorter log");

        RaftCore.Outcome<RequestVoteResponse> ok = core.handleRequestVote(new RequestVoteRequest(3, "n2", 1, 2));
        check(ok.value().voteGranted(), "granted: same last term, log at least as long");
        check(ok.actions().get(0) instanceof Action.PersistHardState p && "n2".equals(p.votedFor()),
                "vote persisted before the reply is sent");

        RaftCore.Outcome<RequestVoteResponse> second = core.handleRequestVote(new RequestVoteRequest(3, "n3", 9, 9));
        check(!second.value().voteGranted(), "denied: already voted for n2 in term 3");
        RaftCore.Outcome<RequestVoteResponse> repeat = core.handleRequestVote(new RequestVoteRequest(3, "n2", 1, 2));
        check(repeat.value().voteGranted(), "same candidate retrying is granted again (idempotent)");
    }

    static void oldTermEntriesCommitOnlyIndirectly() {
        System.out.println("§5.4.2 commit safety");
        RaftCore leader = new RaftCore("n1", List.of("n2", "n3"), 2, null, List.of(entry(1, 1)));
        RequestVoteRequest poll = firstPreVote(leader.onElectionTimeout());
        leader.handlePreVoteResponse("n2", poll, new RequestVoteResponse(3, true));
        List<Action> won = leader.handleRequestVoteResponse("n2", new RequestVoteResponse(3, true));
        check(leader.state() == RaftCore.State.LEADER && leader.currentTerm() == 3, "became leader in term 3");
        check(leader.lastLogIndex() == 2, "leader appended a term-3 no-op after the term-1 entry");

        AppendEntriesRequest toN2 = won.stream()
                .filter(a -> a instanceof Action.SendAppendEntries s && s.peerId().equals("n2"))
                .map(a -> ((Action.SendAppendEntries) a).request()).findFirst().orElseThrow();

        // n2 acknowledges ONLY the old-term entry (index 1): leader + n2 = 2 replicas = a quorum by count.
        leader.handleAppendEntriesResponse("n2", toN2, new AppendEntriesResponse(3, true, 1, 0));
        check(leader.commitIndex() == 0, "old-term entry NOT committed by counting replicas");

        // Once the current-term entry (index 2) reaches a quorum, both commit.
        List<Action> actions = leader.handleAppendEntriesResponse("n2", toN2, new AppendEntriesResponse(3, true, 2, 0));
        check(leader.commitIndex() == 2, "current-term entry reached quorum: commitIndex = 2");
        Action.ApplyEntries applied = actions.stream().filter(a -> a instanceof Action.ApplyEntries)
                .map(a -> (Action.ApplyEntries) a).findFirst().orElseThrow();
        check(applied.entries().size() == 2 && applied.entries().get(0).term() == 1,
                "old-term entry committed indirectly, in order");
    }

    static void conflictingEntriesAreOverwritten() {
        System.out.println("Conflict resolution");
        RaftCore follower = new RaftCore("n2", List.of("n1", "n3"), 1, null,
                List.of(entry(1, 1), entry(1, 2), entry(1, 3)));
        AppendEntriesRequest req = new AppendEntriesRequest(2, "n1", 1, 1, List.of(entry(2, 2)), 0);
        RaftCore.Outcome<AppendEntriesResponse> out = follower.handleAppendEntries(req);
        check(out.value().success() && out.value().matchIndex() == 2, "accepted with matchIndex 2");
        check(follower.lastLogIndex() == 2, "uncommitted entries 2 (term 1) and 3 replaced by leader's entry");
        int truncate = -1;
        int append = -1;
        for (int i = 0; i < out.actions().size(); i++) {
            if (out.actions().get(i) instanceof Action.TruncateLogFrom t && t.index() == 2) {
                truncate = i;
            }
            if (out.actions().get(i) instanceof Action.AppendToLog) {
                append = i;
            }
        }
        check(truncate >= 0 && append > truncate, "disk truncate is ordered before the append");

        AppendEntriesRequest gap = new AppendEntriesRequest(2, "n1", 7, 2, List.of(), 0);
        check(!follower.handleAppendEntries(gap).value().success(), "rejected when prevLogIndex would leave a gap");
        AppendEntriesRequest wrongTerm = new AppendEntriesRequest(2, "n1", 2, 1, List.of(), 0);
        check(!follower.handleAppendEntries(wrongTerm).value().success(), "rejected when prevLogTerm mismatches");
        AppendEntriesRequest stale = new AppendEntriesRequest(1, "n3", 0, 0, List.of(), 0);
        check(!follower.handleAppendEntries(stale).value().success(), "rejected when sender's term is stale");
    }

    static RequestVoteRequest firstPreVote(List<Action> actions) {
        return actions.stream().filter(a -> a instanceof Action.SendPreVote)
                .map(a -> ((Action.SendPreVote) a).request()).findFirst().orElseThrow();
    }

    /** A 3-node leader "n1" in term 3 whose log holds only its own no-op, plus the AppendEntries sent to n2. */
    static Object[] electedLeader() {
        RaftCore core = new RaftCore("n1", List.of("n2", "n3"), 2, null, List.of());
        RequestVoteRequest poll = firstPreVote(core.onElectionTimeout());
        core.handlePreVoteResponse("n2", poll, new RequestVoteResponse(3, true));
        List<Action> won = core.handleRequestVoteResponse("n2", new RequestVoteResponse(3, true));
        AppendEntriesRequest toN2 = won.stream()
                .filter(a -> a instanceof Action.SendAppendEntries s && s.peerId().equals("n2"))
                .map(a -> ((Action.SendAppendEntries) a).request()).findFirst().orElseThrow();
        return new Object[]{core, toN2};
    }

    static void preVote() {
        System.out.println("Pre-vote");
        RaftCore c = new RaftCore("n1", List.of("n2", "n3"), 4, "n3", List.of(entry(2, 1), entry(3, 2)));
        List<Action> timeout = c.onElectionTimeout();
        check(c.state() == RaftCore.State.PRE_CANDIDATE, "election timeout starts a pre-vote, not an election");
        check(c.currentTerm() == 4 && "n3".equals(c.votedFor()), "a pre-vote leaves currentTerm and votedFor untouched");
        check(timeout.stream().noneMatch(a -> a instanceof Action.PersistHardState), "a pre-vote persists nothing");
        RequestVoteRequest poll = firstPreVote(timeout);
        check(poll.term() == 5 && poll.lastLogIndex() == 2 && poll.lastLogTerm() == 3,
                "the poll asks for term+1 and carries the log position");

        RaftCore voter = new RaftCore("n2", List.of("n1", "n3"), 4, "n3", List.of(entry(2, 1), entry(3, 2)));
        RaftCore.Outcome<RequestVoteResponse> granted = voter.handlePreVote(poll);
        check(granted.value().voteGranted() && granted.value().term() == 5, "granted for an up-to-date candidate");
        check(voter.currentTerm() == 4 && "n3".equals(voter.votedFor()) && granted.actions().isEmpty(),
                "granting a pre-vote changes no state and persists nothing");
        check(!voter.handlePreVote(new RequestVoteRequest(4, "n1", 2, 3)).value().voteGranted(),
                "denied when the proposed term is not ahead of ours");
        check(!voter.handlePreVote(new RequestVoteRequest(5, "n1", 1, 2)).value().voteGranted(),
                "denied for a stale log");

        List<Action> election = c.handlePreVoteResponse("n2", poll, new RequestVoteResponse(5, true));
        check(c.state() == RaftCore.State.CANDIDATE && c.currentTerm() == 5,
                "a pre-vote majority starts the real election in term+1");
        check(election.get(0) instanceof Action.PersistHardState p && p.term() == 5 && "n1".equals(p.votedFor()),
                "the election persists term and self-vote first");
        check(election.stream().anyMatch(a -> a instanceof Action.SendRequestVote), "the election sends RequestVote");

        RaftCore lagging = new RaftCore("n1", List.of("n2", "n3"), 1, null, List.of());
        RequestVoteRequest laggingPoll = firstPreVote(lagging.onElectionTimeout());
        lagging.handlePreVoteResponse("n2", laggingPoll, new RequestVoteResponse(9, false));
        check(lagging.state() == RaftCore.State.FOLLOWER && lagging.currentTerm() == 9,
                "a denial carrying a higher term makes the poller adopt it");

        RaftCore late = new RaftCore("n1", List.of("n2", "n3"), 1, null, List.of());
        RequestVoteRequest round = firstPreVote(late.onElectionTimeout());
        late.handlePreVoteResponse("n2", round, new RequestVoteResponse(2, true));
        long termAfterElectionStarted = late.currentTerm();
        List<Action> ignored = late.handlePreVoteResponse("n3", round, new RequestVoteResponse(2, true));
        check(late.currentTerm() == termAfterElectionStarted && ignored.isEmpty(),
                "a late pre-vote grant after the election started is ignored");
    }

    static void checkQuorum() {
        System.out.println("Check-quorum");
        Object[] made = electedLeader();
        RaftCore leader = (RaftCore) made[0];
        AppendEntriesRequest toN2 = (AppendEntriesRequest) made[1];
        check(leader.state() == RaftCore.State.LEADER, "n1 is leader");

        leader.handleAppendEntriesResponse("n2", toN2, new AppendEntriesResponse(3, true, 1, 0));
        List<Action> tick1 = leader.onElectionTimeout();
        check(leader.state() == RaftCore.State.LEADER, "a leader that heard from a majority keeps leading");
        check(tick1.stream().anyMatch(a -> a instanceof Action.ResetElectionTimer), "the tick re-arms itself");

        List<Action> tick2 = leader.onElectionTimeout();
        check(leader.state() == RaftCore.State.FOLLOWER && leader.leaderId() == null,
                "a leader that heard from nobody since the last tick steps down");
        check(leader.currentTerm() == 3, "stepping down by check-quorum does not change the term");
        check(tick2.stream().anyMatch(a -> a instanceof Action.ResetElectionTimer), "it re-arms the timer as a follower");
    }

    static void leaderStickiness() {
        System.out.println("Leader stickiness");
        List<LogEntry> log = List.of(entry(3, 1));
        AppendEntriesRequest heartbeat = new AppendEntriesRequest(4, "n3", 1, 3, List.of(), 0);
        RequestVoteRequest poll = new RequestVoteRequest(5, "n1", 1, 3);

        RaftCore sticky = new RaftCore("n2", List.of("n1", "n3"), 4, null, log, true);
        sticky.handleAppendEntries(heartbeat);
        check(!sticky.handlePreVote(poll).value().voteGranted(), "sticky: a node that heard a live leader denies pre-votes");
        sticky.onElectionTimeout();
        check(sticky.handlePreVote(poll).value().voteGranted(), "sticky: after its own election timer fires it grants again");

        RaftCore plain = new RaftCore("n2", List.of("n1", "n3"), 4, null, log);
        plain.handleAppendEntries(heartbeat);
        check(plain.handlePreVote(poll).value().voteGranted(), "non-sticky: grants even right after a heartbeat");
    }
}
